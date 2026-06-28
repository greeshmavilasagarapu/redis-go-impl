package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func main() {
	store := make(map[string]string)

	// Start the TCP server on port 6379
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("Failed to bind to port 6379: %v\n", err)
	}
	defer listener.Close()

	fmt.Println("Redis server listening on port 6379...")

	for {
		// Accept incoming connections
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// Handle the connection concurrently
		go handleConnection(conn, store)
	}
}

func handleConnection(conn net.Conn, store map[string]string) {
	defer conn.Close()

	buf := make([]byte, 512)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v\n", err)
			}
			break
		}

		request := string(buf[:n])
		reqArr := strings.Split(request, "\r\n")

		fmt.Println(reqArr)

		switch reqArr[2] {
		case "ping", "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "echo", "ECHO":
			conn.Write([]byte("+" + reqArr[4] + "\r\n"))
		case "set", "SET":
			store[reqArr[4]] = reqArr[6]
			conn.Write([]byte("+OK\r\n"))
		case "get", "GET":
			value := store[reqArr[4]]
			if value != "" {
				conn.Write([]byte("+" + value + "\r\n"))
			} else {
				conn.Write([]byte("_" + "\r\n"))
			}
		case "del", "DEL":
			if store[reqArr[4]] == "" {
				conn.Write([]byte(":-1\r\n"))
			} else {
				conn.Write([]byte(":1\r\n"))
				store[reqArr[4]] = ""
			}

		case "setnx", "SETNX":
			if store[reqArr[4]] == "" {
				store[reqArr[4]] = reqArr[6]
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "exists", "EXISTS":
			c := 0
			for i := 4; i < len(reqArr); i += 2 {
				_, exists := store[reqArr[i]]
				if exists {
					c++
				}
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", c)))
		case "keys", "KEYS":
			if reqArr[4] == "*" {
				for key := range store {
					conn.Write([]byte("+" + key + "\r\n"))
				}
			}
		case "append", "APPEND":
			store[reqArr[4]] += reqArr[6]
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(store[reqArr[4]]))))
		case "strlen", "STRLEN":
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", len(store[reqArr[4]]))))
		case "mset", "MSET":
			for i := 4; i < len(reqArr); i += 4 {
				store[reqArr[i]] = reqArr[i+2]

			}
			conn.Write([]byte("+OK\r\n"))
		case "mget", "MGET":
			for i := 4; i < len(reqArr); i += 2 {
				j, ex := store[reqArr[i]]
				if ex {
					conn.Write([]byte("+" + j + "\r\n"))
				} else {
					conn.Write([]byte("_\r\n"))
				}
			}
		case "incr", "INCR":
		case "hset", "HSET":
			//store[reqArr[4]] = reqArr[8]
			c := 0
			for i := 6; i < len(reqArr); i += 4 {
				_, m := store[reqArr[i]]
				if !m {
					c++
				}
				store[reqArr[i]] = reqArr[i+2]
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", c)))
		case "hget", "HGET":
			value, exists := store[reqArr[6]]
			if exists {
				conn.Write([]byte("+" + value + "\r\n"))
			} else {
				conn.Write([]byte("_\r\n"))
			}
			//case "hgetall", "HGETALL":
			//for field, value := range store[reqArr[4]] {
			//conn.Write([]byte("+" + field + "\r\n"))
			//conn.Write([]byte("+" + value + "\r\n"))
			//}
		case "hmset", "HMSET":
			store[reqArr[6]] = reqArr[8]
			conn.Write([]byte("+OK\r\n"))
		case "hsetnx", "HSETNX":
			if store[reqArr[6]] == "" {
				store[reqArr[6]] = reqArr[8]
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "hdel", "HDEL":
			if store[reqArr[6]] == "" {
				conn.Write([]byte(":0\r\n"))
			} else {
				conn.Write([]byte(":1\r\n"))
				store[reqArr[6]] = ""
			}

		case "hkeys", "HKEYS":
			//if reqArr[6] == "" {
			for key := range store {
				conn.Write([]byte("+" + key + "\r\n"))
			}
			//}
		case "hlen", "HLEN":
			c := 0
			for range store {
				c++
			}
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", c)))
		case "hvals", "HVALS":
			for _, value := range store {
				conn.Write([]byte("+" + value + "\r\n"))
			}
		default:
			conn.Write([]byte("-CMD NOT FOUND\r\n"))
		}

		fmt.Println(store)
	}
}

// *2
// $7
// COMMAND
// $4
// DOCS

// ["COMMAND", "DOCS"]
