package main

import (
  "github.com/johnietre/pubsub/server"
)

func main() {
  s, err := server.NewServer("127.0.0.1:8000")
  if err != nil {
    panic(err)
  }
  panic(s.Run())
}
