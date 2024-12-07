package src

import (
	"encoding/json"
	"my_redis/public"
)

func (s *Server) Deal() {
	for {
		ch := <-s.DealQueue
		// 解析数据
		//获取第一个uint8的数据为请求类型
		data := <-ch
		Type := uint8(data[0])
		// 处理请求
		// fmt.Println("Type:", Type)
		switch Type {
		case SET:
			s.Set(data[1:], ch)
		case GET:
			s.Get(data[1:], ch)
		default:
			ch <- []byte("请求类型错误")
		}
	}
}

func (s *Server) Set(data []byte, ch chan []byte) {
	// 解析数据
	req := new(public.SetReq)
	// 反序列化
	err := json.Unmarshal(data, req)
	if err != nil {
		// 返回错误
		ch <- []byte(err.Error())
		//放回空闲队列
		s.DisengagedQueue <- ch
		return
	}
	// 处理请求
	s.M[req.Key] = req.Value
	// 返回结果(0代表成功)
	ch <- []byte("0")
	//放回空闲队列
	s.DisengagedQueue <- ch
}

func (s *Server) Get(data []byte, ch chan []byte) {
	// 解析数据
	req := new(public.GetReq)
	// 反序列化
	err := json.Unmarshal(data, req)
	if err != nil {
		// 返回错误
		ch <- []byte(err.Error())
		//放回空闲队列
		s.DisengagedQueue <- ch
		return
	}
	// 处理请求
	value := s.M[req.Key]
	// 返回结果
	resp := new(public.GetResp)
	resp.Value = value
	// 序列化
	data, err = json.Marshal(resp)
	if err != nil {
		// 返回错误
		ch <- []byte(err.Error())
		//放回空闲队列
		s.DisengagedQueue <- ch
		return
	}
	ch <- data
	//放回空闲队列
	s.DisengagedQueue <- ch
}
