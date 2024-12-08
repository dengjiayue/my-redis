package src

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"my_redis/public"
	"net"
)

// 获取一个请求chan
func (s *Server) GetChan() chan []byte {
	// 获取队列的一个请求chan(阻塞等待)
	rsp := <-s.DisengagedQueue
	return rsp
}

// 向通道写入数据, 并将通道放入处理队列
func (s *Server) Request(rsp chan []byte, data []byte) {
	// 放入处理队列
	s.DealQueue <- rsp
	// 向通道写入数据
	rsp <- data

}

// 启动处理服务,监听10000端口并建立tcp连接
func (s *Server) Run(address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal("Error starting server:", err)
	}
	defer listener.Close()
	log.Printf("Server listening on %s...\n", address)

	// 等待并处理客户端连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("New client connected:", conn.RemoteAddr())

		// 每个连接启动一个 goroutine 来处理
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := io.Reader(conn)
	// 读取客户端的消息
	for {
		// 读取长度
		len, tag, err := ReadHeader(reader)
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from connection:", err)
			}
			return
		}
		// fmt.Println("len:", len)
		// 读取数据
		data, err := ioutil.ReadAll(io.LimitReader(reader, int64(len)))
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from connection:", err)
			}
			return
		}
		go func(t uint32, reqdata []byte) { // 获取一个请求chan
			rsp := s.GetChan()
			// 向通道写入数据
			s.Request(rsp, reqdata)
			// 读取通道数据
			rspData := PackData(t, <-rsp)
			// 放入空闲队列
			s.DisengagedQueue <- rsp
			// 发送数据
			_, err = conn.Write(rspData)
			if err != nil {
				log.Println("Error writing to connection:", err)
			}
			// fmt.Println("Response sent:", string(rspData))
		}(tag, data)
	}
}

// uint32转bytes
func Uint32ToBytes(n uint32) []byte {
	return []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

// bytes转uint32
func BytesToUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// 解码数据头
func ReadHeader(reader io.Reader) (uint32, uint32, error) {

	b := make([]byte, 8)
	_, err := reader.Read(b)
	if err != nil {
		return 0, 0, err
	}

	l, t := public.Uncode(b)
	return l, t, nil
}

// 打包数据(长度(4byte)+tag+数据)
func PackData(tag uint32, data []byte) []byte {
	b := public.Encode(tag, data)
	return b
}
