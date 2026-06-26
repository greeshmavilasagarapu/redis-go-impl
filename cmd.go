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
