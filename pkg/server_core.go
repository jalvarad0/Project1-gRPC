package whatsup

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const BATCH_SIZE = 50
const MAILBOX_SIZE = 1024

// A simple hash function for Connect to use to generate new tokens.
// Not used anywhere else.
func hash(name string) (result string) {
	return fmt.Sprintf("%x", md5.Sum([]byte(name)))
}

// The gRPC implementation of our Server
type Server struct {
	UnimplementedWhatsUpServer
	// A map of auth tokens to strings
	AuthToUserTable map[string]string
	// A map of users to messages in their inbox. The inbox is modelled
	// as a buffered channel of size MAILBOX_SIZE.
	Inboxes map[string](chan *ChatMessage)
}

func NewServer() Server {
	return Server{
		AuthToUserTable: make(map[string]string),
		Inboxes:         make(map[string](chan *ChatMessage)),
	}
}

// A server-side interceptor that maps the authentication tokens in our `context` back to usernames.
// Rejects calls if they don't have a valid authentication token. Note: we've made our interceptor
// in this case a method on our Server struct so that it can have access to the Server's private variables
// - however, this is not a strict requirement for interceptors in general.
func (s Server) Interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

	// allow calls to Connect endpoint to pass through
	if info.FullMethod == "/whatsup.WhatsUp/Connect" {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("Couldn't read metadata for request")
	}

	// if token is present in metadata
	if values, ok := md["token"]; ok {
		if len(values) == 1 {
			// if user is present in s.AuthToUserTable
			if user, ok := s.AuthToUserTable[values[0]]; ok {
				return handler(context.WithValue(context.Background(), "username", user), req)
			}
		}
	}

	return nil, errors.New("Could not fetch user from authentication token, if provided")
}

// Implementation of the Connect method defined in our `.proto` file.
// Converts the username provided by `Registration` to an `AuthToken` object.
// The token returned is unique to the user - if the user is already logged in,
// the connect will should be rejected. This function creates a corresponding entry
// in `s.AuthToUserTable` and `s.Inboxes`.
func (s Server) Connect(_ context.Context, r *Registration) (*AuthToken, error) {

	token := hash(r.SourceUser)

	if _, ok := s.AuthToUserTable[token]; !ok {
		s.AuthToUserTable[token] = r.SourceUser
		s.Inboxes[r.SourceUser] = make(chan *ChatMessage, MAILBOX_SIZE)

		return &AuthToken{
			Token: token,
		}, nil
	}

	return nil, errors.New("User is already logged in")

}

// Implementation of the Send method defined in our `.proto` file.
// Should write the chat message to a target user's private inbox in s.Inboxes.
// The chat message should have its `User` field replaced with the sending user
// (when you initially receive it, it will have the name of the recipient instead).
// TODO: Implement `Send`. If any errors occur, return any error message you'd like.
func (s Server) Send(ctx context.Context, msg *ChatMessage) (*Success, error) {
	// First extract the targets name
	src := fmt.Sprintf("%v", ctx.Value("username"))
	if src == "" {
		return nil, errors.New("unauthenticated: missing username in context")
	}

	// Extract the targets name
	dest := msg.User
	if dest == "" {
		return nil, errors.New("unauthenticated: missing receiver in context")
	}

	// Next grab their inbox
	inbox, ok := s.Inboxes[dest]
	if !ok {
		return nil, errors.New("unauthenticated: We don't have an inbox for that receiver")
	}

	// I then want to update the msg use field so that they know who it is from
	msg.User = src

	// I then want to put it in their mailbox
	select {
	case inbox <- msg: // This syntax means, send a message to this inbox
		return &Success{Ok: true}, nil
	default: // Case where we fail
		return &Success{Ok: false}, errors.New("We were not able to successfully send the message")
	}
}

// Implementation of the Fetch method defined in our `.proto` file.
// Should consume all messages from the inbox channel for the current user
// in batches of BATCH_SIZE. Hint: use `select` statements in a suitable `for`
// loop to consume from the channel until some condition is reached, since you
// don't want to accidentally miss messages.
//
// TODO: Implement Fetch. If any errors occur, return any error message you'd like.
func (s Server) Fetch(ctx context.Context, _ *Empty) (*ChatMessages, error) {

	// Lets grab the targets name
	target := fmt.Sprintf("%v", ctx.Value("username"))
	if target == "" {
		return nil, errors.New("missing username in context")
	}

	// Now lets grab their mailbox
	inbox, ok := s.Inboxes[target]
	if !ok {
		return nil, errors.New("We don't have an inbox for that receiver")
	}

	// Now we create our messages buffer. Note to self, this syntax in go is like initalizing a pointer to a struct
	messages := &ChatMessages{Messages: []*ChatMessage{}}

	// Next we will for loop through the messages and add them to out buffer using select. Handle errors
	for len(messages.Messages) < BATCH_SIZE { // Loop until we have BATCH_SIZE amout of messages in our messages bucket. Similar to while loop
		select {
		case <-ctx.Done(): // This is the case where the client disconnected or deadline expired
			if len(messages.Messages) > 0 {
				return messages, nil
			}
			return nil, errors.New("Something went wrong with with the connection")
		case msg, open := <-inbox: // This is the case where we grab a message from the inbox. Note open allows us to tell if messages had stuff.
			if !open {
				return messages, nil
			}
			messages.Messages = append(messages.Messages, msg)
		default:
			return messages, nil
		}
	}
	// We successfully got the messages out!
	return messages, nil
}

// Implementation of the List method defined in our `.proto` file.
// Should consume from the inbox channel for the current user.
func (s Server) List(ctx context.Context, _ *Empty) (*UserList, error) {

	u := &UserList{
		Users: []string{},
	}

	for _, user := range s.AuthToUserTable {
		u.Users = append(u.Users, user)
	}

	return u, nil

}

// Implementation of the Disconnect method defined in our `.proto` file.
// Should destroy the corresponding inbox and entry in `s.AuthToUserTable`
func (s Server) Disconnect(ctx context.Context, _ *Empty) (*Success, error) {

	user := fmt.Sprintf("%v", ctx.Value("username"))
	close(s.Inboxes[user]) // make sure no more writes can be sent on this channel
	delete(s.Inboxes, user)

	for token, u := range s.AuthToUserTable {
		if u == user {
			delete(s.AuthToUserTable, token)
		}
	}

	return &Success{Ok: true}, nil
}
