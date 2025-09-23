package main

import (
    "flag"
    "fmt"
    "google.golang.org/grpc"
    whatsup "whatsup/pkg"
)

func main() {

    serverPortPtr := flag.String("port", "", "chat server port to connect to")
    flag.Parse()

    listen, port, err := whatsup.OpenListener(*serverPortPtr)
    fmt.Printf("Listening on port %s\n", port)

    if err != nil {
        fmt.Println(err)
        return
    }

    whatsupService := whatsup.NewServer()

    realServer := grpc.NewServer(
        grpc.UnaryInterceptor(whatsupService.Interceptor),
    )
    whatsup.RegisterWhatsUpServer(realServer, whatsupService)
    if err := realServer.Serve(listen); err != nil {
        fmt.Printf("failed to serve: %v", err)
    }
}
