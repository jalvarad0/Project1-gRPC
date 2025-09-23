package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	whatsup "whatsup/pkg"
)

const (
	DEFAULT_SERVER_ADDR        string = "localhost"
	DEFAULT_SERVER_PORT        string = "12345"
	DEFAULT_USERNAME           string = "jeffra"
	DEFAULT_TIMEOUT_IN_SECONDS int    = 3
)

func main() {

	userPtr := flag.String("user", "", "username used for this chat client")
	serverPortPtr := flag.String("port", "", "chat server port to connect to")
	serverAddrPtr := flag.String("addr", "", "address to chat server")
	flag.Parse()

	start(*userPtr, *serverPortPtr, *serverAddrPtr)
}

func start(user string, serverPort string, serverAddr string) {

	if user == "" {
		user = DEFAULT_USERNAME
	}
	if serverAddr == "" {
		serverAddr = DEFAULT_SERVER_ADDR
	}
	if serverPort == "" {
		serverPort = DEFAULT_SERVER_PORT
	}

	address := fmt.Sprintf("%s:%s", serverAddr, serverPort)
	conn, client, ctx, err := whatsup.ClientSetup(address, user, DEFAULT_TIMEOUT_IN_SECONDS)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// print welcome message and begin REPL environment
	fmt.Printf("Welcome %s. Try any of the following commands\n", user)
	fmt.Println("\t fetch - See any new messages since last update")
	fmt.Println("\t list - See all logged-in users")
	fmt.Println("\t quit - Disconnect")
	fmt.Println("\t <user> <message...> - Send <message> to <user>")

	for {
		fmt.Printf(fmt.Sprintf("%s@ ", user))
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		args := strings.SplitN(line, " ", 2)

		response, err := whatsup.Execute(client, ctx, args...)
		if err != nil {
			fmt.Printf("%s\n", err)
			switch err.(type) {
			case *whatsup.DisconnectError:
				return
			default:
			}
		}
		fmt.Printf("%s", response)
	}
}
