package whatsup

//__BEGIN_TA__
/* TA Code */

import (
    "context"
    "errors"
    "fmt"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    "strings"
    "time"
)

type DisconnectError struct {
    AdditionalReasons string
}

func (e *DisconnectError) Error() string {
    return fmt.Sprintf("Server has been disconnected - errors, if any %s", e.AdditionalReasons)
}

/*
   Register as new user with the upstream server. Fetch the
   authentication token using client.Connect() and store it
   in a `context` object. We are validating this context
   object on the server side.

   TODO: Implement `Register`. You should call the `Connect`
   RPC and use the `metadata` package appropriately to place
   the auth token returned into a context.Context object.
   Take a look at the server-side interceptor in `server_core.go`
   to understand what the structure of the context.Context object
   should look like.

   If any errors occur, return any error message you'd like.
*/
func Register(client WhatsUpClient, user string) (context.Context, error) {

}

// A helper function that returns an active client connection to the
// WhatsUp server.
func ClientSetup(address string, user string, timeout int) (*grpc.ClientConn, WhatsUpClient, context.Context, error) {

    // Establish a connection to the chat server
    timeoutInSeconds := time.Duration(timeout) * time.Second

    connection, err := grpc.Dial(
        address,
        // gRPC options that indicate we should connect with
        // plain TCP and block until the connection is established
        grpc.WithInsecure(),
        grpc.WithBlock(),
        // timeout after some seconds if cannot connect in that time
        grpc.WithTimeout(timeoutInSeconds),
    )
    if err != nil {
        return &grpc.ClientConn{}, nil, nil, errors.New(fmt.Sprintf("unable to connect to server: %s\n", err))
    }

    client := NewWhatsUpClient(connection)

    // register our client as a new user
    ctx, err := Register(client, user)
    if err != nil {
        return &grpc.ClientConn{}, nil, nil, errors.New(fmt.Sprintf("unable to register with server: %s\n", err))
    }

    return connection, client, ctx, nil

}

// A helper function that carries out the actions indicated by the arguments.
// Arguments can either be a one-element slice or a two-element slice of strings.
// If it contains two elements, the client sends a message to the server - the first
// element is treated as the user to send to, and the second element is the complete
// message to be sent. It returns a string to display to the user containing the results.
// of the operation.
func Execute(client WhatsUpClient, ctx context.Context, arguments ...string) (string, error) {

    if len(arguments) == 1 {
        switch arguments[0] {

        case "fetch":

            messages, err := client.Fetch(ctx, &Empty{})
            if err != nil {
                return "", err
            }

            all := []string{}
            for _, message := range messages.Messages {
                all = append(all, fmt.Sprintf("[%s]: %s", message.User, message.Body))
            }

            return fmt.Sprintf("%s\n", strings.Join(all, "\n")), nil

        case "list":

            // TODO: Implement the client RPC call for List!
            // This should print a comma-separated string of all users returned by
            // the RPC, ending with a newline character "\n", to the console.
            // The order of the users printed does not matter.

        case "quit":

            success, err := client.Disconnect(ctx, &Empty{})
            if err != nil || !success.Ok {
                return "", &DisconnectError{AdditionalReasons: err.Error()}
            }
            return "", &DisconnectError{AdditionalReasons: ""}
        }
    }

    if len(arguments) == 2 {
        success, err := client.Send(ctx, &ChatMessage{
            User: arguments[0],
            Body: arguments[1],
        })

        if err != nil || !success.Ok {
            return "", errors.New(fmt.Sprintf("Failed to send - errors, if any: %s", err))
        }
    }

    return "", nil

}
