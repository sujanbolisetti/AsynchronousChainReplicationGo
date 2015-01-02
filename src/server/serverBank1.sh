#!/bin/bash

go run serverBank1.go 0 &
go run serverBank1.go 1 &
go run serverBank1.go 2 &
go run serverBank1.go 3 &
go run serverBank1.go 4 &
wait
echo "All the servers are closed"
