package main

import (
  "fmt"
  "strconv"
  "time"

  psserver "github.com/johnietre/pubsub/server"
)

var dur = time.Second * 5

var server = psserver.NewLocalServer()

func main() {
  client := server.NewClient(5, false)
  defer client.Close()
  err := client.NewChan("chan3", "chan5", "chan15", "chann"); must(err)
  chann, ok := client.GetChan("chann")
  if !ok {
    panic("channel not loaded")
  }
  go goSub()
  time.Sleep(dur)
  for i := 0; i <= 100; i++ {
    if i % 15 == 0 {
      client.Pub("fizzbuzz", "chan15")
    } else if i % 3 == 0 {
      client.Pub("fizz", "chan3")
    } else if i % 5 == 0 {
      client.Pub("buzz", "chan5")
    } else {
      chann.Pub(strconv.Itoa(i))
    }
  }
  client.Pub("done", "chan3", "chan5", "chan15", "chann")
  time.Sleep(dur)
}

func goSub() {
  client := server.NewClient(100, false)
  defer client.Close()
  client.Sub("chan3", "chan5", "chan15", "chann")

  ct, err := client.RefreshAllChanNames()
  if err != nil {
    panic(err)
  }
  if !client.WaitForChansRefresh(ct, 5) {
    panic("no refresh")
  }
  client.GetAllChanNames().Range(func(name string) bool {
    fmt.Printf("Chan name: %s\n", name)
    return true
  })

  for i := 0; i < 100; i++ {
    name, msg, _, err := client.Recv(true)
    if err != nil {
      fmt.Printf("error with %s: %v", name, err)
      return
    }
    fmt.Printf("%s: %s\n", name, msg)
  }
  name, msg, got, err := client.Recv(false)
  if err != nil {
    fmt.Printf("error with %s: %v", name, err)
    return
  }
  if !got {
    fmt.Println(false)
    return
  }
  fmt.Printf("%s: %s\n", name, msg)
}

func must(err error) {
  if err != nil {
    panic(err)
  }
}
