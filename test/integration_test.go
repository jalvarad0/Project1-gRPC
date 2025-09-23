package whatsup

import (
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"math/rand"
	"testing"
	"time"
	whatsup "whatsup/pkg"
)

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

// generate a random string of a certain length
func randomString(len int) string {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(randInt(97, 122))
	}
	return string(bytes)
}

// Test authentication tokens are issued and stored when a client contacts
// a server.
func TestSingleUserAuth(t *testing.T) {

	user := randomString(12)

	whatsupService := whatsup.NewServer()
	realServer := grpc.NewServer(
		grpc.UnaryInterceptor(whatsupService.Interceptor),
	)

	if len(whatsupService.AuthToUserTable) != 0 {
		t.Errorf("Expected one element in AuthToUserTable, found %+v", whatsupService.AuthToUserTable)
	}

	listen, port, _ := whatsup.OpenListener("")
	address := fmt.Sprintf("localhost:%s", port)

	go func() {
		whatsup.RegisterWhatsUpServer(realServer, whatsupService)
		if err := realServer.Serve(listen); err != nil {
			t.Fatalf(err.Error())
		}
	}()

	defer func() {
		realServer.GracefulStop()
	}()

	conn, _, ctx, err := whatsup.ClientSetup(address, user, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer conn.Close()

	if len(whatsupService.AuthToUserTable) != 1 {
		t.Errorf("Expected one element in AuthToUserTable, found %+v", whatsupService.AuthToUserTable)
	}

	for _, value := range whatsupService.AuthToUserTable {
		if value != user {
			t.Errorf("Expected user %s to be in user table, but user table had %+v", user, whatsupService.AuthToUserTable)
		}
	}

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Errorf("Expected context to have metadata, got %+v", ctx)
	}

	if _, retrieved := md["token"]; !retrieved {
		t.Errorf("Expected metadata to have token, got %+v", md)
	}
}

// Test single client can interact with the server
func TestSingleUserInteraction(t *testing.T) {

	user := randomString(12)
	whatsupService := whatsup.NewServer()
	realServer := grpc.NewServer(
		grpc.UnaryInterceptor(whatsupService.Interceptor),
	)

	listen, port, _ := whatsup.OpenListener("")
	address := fmt.Sprintf("localhost:%s", port)

	go func() {
		whatsup.RegisterWhatsUpServer(realServer, whatsupService)
		if err := realServer.Serve(listen); err != nil {
			t.Fatalf(err.Error())
		}
	}()

	defer func() {
		realServer.GracefulStop()
	}()

	conn, client, ctx, err := whatsup.ClientSetup(address, user, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer conn.Close()

	answer, err := whatsup.Execute(client, ctx, "list")
	expected := fmt.Sprintf("%s\n", user)
	if answer != expected || err != nil {
		t.Errorf("Expected %q on call `list`, got %q with err %+v", expected, answer, err)
	}

	whatsup.Execute(client, ctx, user, "hello")
	message, err := whatsup.Execute(client, ctx, "fetch")
	expected = fmt.Sprintf("[%s]: hello\n", user)
	if message != expected || err != nil {
		t.Errorf("Expected %q on call `fetch` after one message, got %q with err %+v", expected, message, err)
	}

	whatsup.Execute(client, ctx, user, "multipart message 1")
	whatsup.Execute(client, ctx, user, "multipart message 2")
	messages, err := whatsup.Execute(client, ctx, "fetch")
	expected = fmt.Sprintf("[%s]: multipart message 1\n[%s]: multipart message 2\n", user, user)
	if messages != expected || err != nil {
		t.Errorf("Expected %q on call `fetch` after multiple messages, got %q with err %+v", expected, messages, err)
	}

	expected = ""
	for i := 0; i < 2*whatsup.BATCH_SIZE; i++ {
		whatsup.Execute(client, ctx, user, fmt.Sprintf("%d", i))
		if i < whatsup.BATCH_SIZE {
			expected += fmt.Sprintf("[%s]: %d\n", user, i)
		}
	}
	messages, err = whatsup.Execute(client, ctx, "fetch")
	if messages != expected {
		t.Errorf("Expected %s, got %s when requesting messages more the batch size", expected, messages)
	}
}

// Test single client can interact with the server
func TestMultipleClients(t *testing.T) {

	userOne := randomString(12)
	userTwo := randomString(12)

	whatsupService := whatsup.NewServer()
	realServer := grpc.NewServer(
		grpc.UnaryInterceptor(whatsupService.Interceptor),
	)

	listen, port, _ := whatsup.OpenListener("")
	address := fmt.Sprintf("localhost:%s", port)

	go func() {
		whatsup.RegisterWhatsUpServer(realServer, whatsupService)
		if err := realServer.Serve(listen); err != nil {
			t.Fatalf(err.Error())
		}
	}()

	defer func() {
		realServer.GracefulStop()
	}()

	connOne, clientOne, ctxOne, err := whatsup.ClientSetup(address, userOne, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer connOne.Close()

	connTwo, clientTwo, ctxTwo, err := whatsup.ClientSetup(address, userTwo, 3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer connTwo.Close()

	answer, err := whatsup.Execute(clientOne, ctxOne, "list")
	// students may implement this in an order-independent fashion
	expectedA := fmt.Sprintf("%s,%s\n", userOne, userTwo)
	expectedB := fmt.Sprintf("%s,%s\n", userTwo, userOne)
	if (answer != expectedA && answer != expectedB) || err != nil {
		t.Errorf("Expected either %q or %q on call `list`, got %q with err %+v", expectedA, expectedB, answer, err)
	}

	expected := ""
	for i := 0; i < 2*whatsup.BATCH_SIZE; i++ {
		whatsup.Execute(clientTwo, ctxTwo, userOne, fmt.Sprintf("%d", i))
		if i < whatsup.BATCH_SIZE {
			expected += fmt.Sprintf("[%s]: %d\n", userTwo, i)
		}
	}
	messages, err := whatsup.Execute(clientOne, ctxOne, "fetch")
	if messages != expected {
		t.Errorf("Expected %s, got %s when requesting messages more than the batch size", expected, messages)
	}
}
