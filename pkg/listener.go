package whatsup

/* Library Code */

import (
	"math/rand"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"
)

// Ephemeral port range on Brown machines
const LOW_PORT int = 32768
const HIGH_PORT int = 61000

// Errno to support windows machines
const WIN_EADDRINUSE = syscall.Errno(10048)

// Listens on a random port in the defined ephemeral range, retries if port is already in use
func OpenListener(requested_port string) (net.Listener, string, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	port := strconv.Itoa(rand.Intn(HIGH_PORT-LOW_PORT) + LOW_PORT)
	if requested_port != "" {
		port = requested_port
	}
	conn, err := net.Listen("tcp", ":"+port)
	if err != nil {
		if addrInUse(err) {
			time.Sleep(100 * time.Millisecond)
			return OpenListener("")
		} else {
			return nil, "", err
		}
	}
	return conn, port, err
}

func addrInUse(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if osErr, ok := opErr.Err.(*os.SyscallError); ok {
			return osErr.Err == syscall.EADDRINUSE || osErr.Err == WIN_EADDRINUSE
		}
	}
	return false
}
