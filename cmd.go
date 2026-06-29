package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	store := make(map[string]string)
	hstore := make(map[string]map[string]string)
	expiry := make(map[string]time.Time)
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
		go handleConnection(conn, store, hstore, expiry)
	}
}

func handleConnection(conn net.Conn, store map[string]string, hstore map[string]map[string]string, expiry map[string]time.Time) {
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
		case "msetnx", "MSETNX":
			flag := true
			for i := 4; i < len(reqArr); i += 4 {
				if store[reqArr[i]] != "" {
					flag = false
				}
			}
			if flag {
				for i := 4; i < len(reqArr); i += 4 {
					store[reqArr[i]] = reqArr[i+2]
				}
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "flushall", "FLUSHALL":
			store = make(map[string]string)
			hstore = make(map[string]map[string]string)
			conn.Write([]byte("+OK\r\n"))

		case "incr", "INCR":
			// key is at position 4 in the parsed request
			key := reqArr[4]
			valStr := store[key]
			if valStr == "" {
				valStr = "0"
			}
			v, err := strconv.Atoi(valStr)
			if err != nil {
				conn.Write([]byte("-Error: Not a number\r\n"))
				break
			}
			v++
			store[key] = strconv.Itoa(v)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
		case "incrby", "INCRBY":
			key := reqArr[4]
			valStr := store[key]
			if valStr == "" {
				valStr = "0"
			}
			v, err := strconv.Atoi(valStr)
			if err != nil {
				conn.Write([]byte("-Error: Not a number\r\n"))
				break
			}
			inc, err := strconv.Atoi(reqArr[6])
			if err != nil {
				conn.Write([]byte("-Error: Increment is not a number\r\n"))
				break
			}
			v += inc
			store[key] = strconv.Itoa(v)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
		case "decr", "DECR":
			v, _ := strconv.Atoi(store[reqArr[4]])
			v--
			store[reqArr[4]] = strconv.Itoa(v)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
		case "decrby", "DECRBY":
			v, _ := strconv.Atoi(store[reqArr[4]])
			dec, _ := strconv.Atoi(reqArr[6])
			v -= dec
			store[reqArr[4]] = strconv.Itoa(v)
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", v)))
		case "expire", "EXPIRE":
			seconds, _ := strconv.Atoi(reqArr[6])
			expiry[reqArr[4]] = time.Now().Add(time.Duration(seconds) * time.Second)
			conn.Write([]byte(":1\r\n"))
		case "ttl", "TTL":
			exp, exists := expiry[reqArr[4]]
			if !exists {
				conn.Write([]byte(":-1\r\n"))
			} else {
				ttl := int(time.Until(exp).Seconds())
				if ttl < 0 {
					conn.Write([]byte(":-2\r\n"))
				} else {
					conn.Write([]byte(fmt.Sprintf(":%d\r\n", ttl)))
				}
			}
		case "persist", "PERSIST":
			_, exists := expiry[reqArr[4]]
			if exists {
				delete(expiry, reqArr[4])
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}
		case "setex", "SETEX":
			store[reqArr[4]] = reqArr[8]
			seconds, _ := strconv.Atoi(reqArr[6])
			expiry[reqArr[4]] = time.Now().Add(time.Duration(seconds) * time.Second)
			conn.Write([]byte("+OK\r\n"))
		case "hset", "HSET":
			hstore[reqArr[2]][reqArr[4]] = reqArr[6]
			conn.Write([]byte(fmt.Sprintf(":%d\r\n", reqArr[6])))
		case "hget", "HGET":
			value, exists := hstore[reqArr[2]][reqArr[4]]
			if exists {
				conn.Write([]byte("+" + value + "\r\n"))
			} else {
				conn.Write([]byte("_\r\n"))
			}
		case "hgetall", "HGETALL":
			for key, value := range hstore[reqArr[4]] {
				conn.Write([]byte("+" + key + "\r\n"))
				conn.Write([]byte("+" + value + "\r\n"))
			}
		case "hmget", "HMGET":
			for i := 6; i < len(reqArr); i += 2 {
				value, exists := hstore[reqArr[4]][reqArr[i]]
				if exists {
					conn.Write([]byte("+" + value + "\r\n"))
				} else {
					conn.Write([]byte("_\r\n"))
				}
			}
		case "hmset", "HMSET":
			for i := 4; i < len(reqArr); i += 4 {
				hstore[reqArr[2]][reqArr[i]] = reqArr[i+2]
			}
			conn.Write([]byte("+OK\r\n"))
		case "hsetnx", "HSETNX":
			if hstore[reqArr[2]][reqArr[4]] == "" {
				hstore[reqArr[2]][reqArr[4]] = reqArr[6]
				conn.Write([]byte(":1\r\n"))
			} else {
				conn.Write([]byte(":0\r\n"))
			}

		case "hdel", "HDEL":
			if hstore[reqArr[2]][reqArr[4]] == "" {
				conn.Write([]byte(":0\r\n"))
			} else {
				conn.Write([]byte(":1\r\n"))
				hstore[reqArr[2]][reqArr[4]] = ""
			}

		case "hkeys", "HKEYS":
			//if reqArr[6] == "" {
			for key := range hstore[reqArr[2]] {
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
			for _, value := range hstore[reqArr[2]] {
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
