package tikv

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
)

type turboConn struct {
	conn     net.Conn
	writer   *bufio.Writer
	reader   *bufio.Reader
	inflight sync.Map
	client   *turboClient
}

type turboClient struct {
	conns    []*turboConn
	reqCh    chan *reqWithWG
	reqID    uint32
	connIdx  int
}

type Packet struct {
	RequestHeader
	ReqBuf     []byte
	Resp       *Response
	wg         *sync.WaitGroup
	err        error
}

type Response struct {
	ID         uint32
	kvResp     *tikvrpc.Response
}

type ReqBatch struct {
	packets []*Packet
	buf     []byte
}

type RequestHeader struct {
	RequestID     uint32
	RequestLen    uint32
	RegionID      uint64
	StoreID       uint32
	RegionVer     uint32
	RegionConfVer uint32
	Term          uint32
	Flags         uint32
	ReqType       uint32
}

const reqHeaderSize = int(unsafe.Sizeof(RequestHeader{}))
const respHeaderSize = int(unsafe.Sizeof(RespHeader{}))

func (h *RequestHeader) ToBytes() []byte {
	var b []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	hdr.Len = reqHeaderSize
	hdr.Cap = reqHeaderSize
	hdr.Data = uintptr(unsafe.Pointer(h))
	return b
}

const (
	ReqTypeGet uint32 = 1
)

type RespHeader struct {
	RequestID uint32
	Status    uint32
	RespLen   uint32
}

const (
	StatusOK        = 0
	StatusRegionErr = 1
	StatusKeyErr    = 2
)

func dialTurbo(addr string) (*turboClient, error) {
	cnt := int(config.GetGlobalConfig().TiKVClient.GrpcConnectionCount)
	bc := &turboClient{
		conns:    make([]*turboConn, cnt),
		reqCh:    make(chan *reqWithWG, 1024),
	}
	var dialer = net.Dialer{
		Timeout: time.Second,
	}
	for i := range bc.conns {
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}
		bCon := &turboConn{
			conn:   conn,
			writer: bufio.NewWriterSize(conn, 1024 * 1024 * 2),
			reader: bufio.NewReaderSize(conn, 1024 * 1024 * 2),
			client: bc,
		}
		bc.conns[i] = bCon
		go bCon.recvLoop()
	}
	return bc, nil
}

type reqWithWG struct {
	id   uint32
	req  *tikvrpc.Request
	wg   sync.WaitGroup
	resp *tikvrpc.Response
	err  error
}

func (bc *turboClient) sendBoostRequest(req *tikvrpc.Request) (*tikvrpc.Response, error) {
	reqWithWG := &reqWithWG{
		req:  req,
	}
	reqWithWG.wg.Add(1)
	bc.reqCh <- reqWithWG
	reqWithWG.wg.Wait()
	return reqWithWG.resp, reqWithWG.err
}

func (bc *turboClient) allocID() uint32 {
	bc.reqID++
	return bc.reqID
}

func (bc *turboClient) sendLoop() {
	var reqs []*reqWithWG
	for {
		reqs = append(reqs[:0], <-bc.reqCh)
		l := len(bc.reqCh)
		for i := 0; i < l; i++ {
			reqs = append(reqs, <-bc.reqCh)
		}
		conn := bc.conns[bc.connIdx]
		bc.connIdx = (bc.connIdx+1) % len(bc.conns)
		for _, req := range reqs {
			getReq := req.req.Get()
			req.id = bc.allocID()
			ctx := &req.req.Context
			header := RequestHeader{
				RequestID:     req.id,
				RequestLen:    uint32(len(getReq.Key) + 8),
				RegionID:      req.req.RegionId,
				StoreID:       uint32(ctx.Peer.StoreId),
				RegionVer:     uint32(ctx.RegionEpoch.Version),
				RegionConfVer: uint32(ctx.RegionEpoch.ConfVer),
				Term:          uint32(ctx.Term),
				ReqType:       ReqTypeGet,
			}
			_, err := conn.writer.Write(header.ToBytes())
			if err != nil {
				log.S().Error(err.Error())
				return
			}
			var verBuf [8]byte
			binary.LittleEndian.PutUint64(verBuf[:], getReq.Version)
			_, err = conn.writer.Write(verBuf[:])
			if err != nil {
				log.S().Error(err.Error())
				return
			}
			_, err = conn.writer.Write(getReq.Key)
			if err != nil {
				log.S().Error(err.Error())
				return
			}
		}
		for _, req := range reqs {
			conn.inflight.Store(req.id, req)
		}
		err := conn.writer.Flush()
		if err != nil {
			log.S().Error(err.Error())
			return
		}
	}
}

func (bc *turboConn) recvLoop() {
	alloc := new(Allocator)
	for {
		headerBuf, err := bc.reader.Peek(respHeaderSize)
		if err != nil {
			log.S().Error(err)
			return
		}
		header := *(*RespHeader)(unsafe.Pointer(&headerBuf[0]))
		_, err = bc.reader.Discard(respHeaderSize)
		if err != nil {
			panic(err)
		}
		body := alloc.AllocData(int(header.RespLen))
		_, err = io.ReadFull(bc.reader, body)
		if err != nil {
			log.S().Error(err)
			return
		}
		getResp := alloc.AllocGetResponse()
		switch header.Status {
		case StatusOK:
			getResp.Value = body
		case StatusRegionErr:
			regErr := new(errorpb.Error)
			_ = regErr.Unmarshal(body)
			getResp.RegionError = regErr
		case StatusKeyErr:
			keyErr := new(kvrpcpb.KeyError)
			_ = keyErr.Unmarshal(body)
			getResp.Error = keyErr
		}
		resp := alloc.AllocKVResponse()
		resp.Resp = getResp
		val, _ := bc.inflight.Load(header.RequestID)
		req := val.(*reqWithWG)
		bc.inflight.Delete(header.RequestID)
		req.resp = resp
		req.wg.Done()
	}
}

type Allocator struct {
	off  int
	data []byte
}

func (a *Allocator) AllocPacket() *Packet {
	data := a.AllocData(int(unsafe.Sizeof(Packet{})))
	return (*Packet)(unsafe.Pointer(&data[0]))
}

func (a *Allocator) AllocData(size int) []byte {
	if a.off+size > len(a.data) {
		newSize := 64 * 1024
		if newSize < size {
			newSize = size
		}
		a.data = make([]byte, newSize)
		a.off = 0
	}
	data := a.data[a.off : a.off+size]
	a.off += int(size)
	for i := range data {
		data[i] = 0
	}
	return data
}

func (a *Allocator) AllocGetResponse() *kvrpcpb.GetResponse {
	data := a.AllocData(int(unsafe.Sizeof(kvrpcpb.GetResponse{})))
	return (*kvrpcpb.GetResponse)(unsafe.Pointer(&data[0]))
}

func (a *Allocator) AllocKVResponse() *tikvrpc.Response {
	data := a.AllocData(int(unsafe.Sizeof(tikvrpc.Response{})))
	return (*tikvrpc.Response)(unsafe.Pointer(&data[0]))
}
