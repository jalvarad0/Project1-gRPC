# Project 1: gRPC

### Task

Create a gRPC server and client that successfully communicate with each other. [Handout is here](https://docs.google.com/document/d/1kKzu7bQ8CPMh06ZE60QBgZjtddfJWKpjWUT6BRXP1vc/edit?usp=sharing).

### Goals

- Write a proto file that defines an RPC service
- Generate go files from your proto
- Send gRPC requests with client to server

### Requirements

To receive full credit for this lab, you must pass all the tests in <code>integration_test.go</code>. Please do not modify the test file. To submit your lab for grading, please upload to GradeScope. 

<h2>Part 1: Proto</h2>
<ol>
<li>Install protoc on your local machine</li>
<li>Create your protobuf file. Make sure to name the package the same as your other files</li>
<li>Run <code>sh generate_grpc.sh</code> the project directory to generate the go files. This script is pretty handy and I recommend you keep it for yourself ;)</li>
</ol>

<h2>Part 2: Server</h2>
Implement the TODOs

<h2>Part 3: Client</h2>
Implement the TODOs

## Building

- Navigate to either `cmd/client` or `cmd/server` and run `go build .`. This will generate client or server binaries for you respectively. 

## Testing

- `go test ./...` should run all tests for you from within this repository root. 
