package main

import (
  "fmt"
  "sync"
  "time"

  "github.com/johnietre/pubsub"
)

var wg sync.WaitGroup

func main() {
  srvr := pubsub.NewLocalServer()
  c1 := srvr.NewClient(5, false)
  c2 := srvr.NewClient(5, false)
  c3 := srvr.NewClient(5, false)

  c1.NewMultiChan("c1:c2:c3")
  c2.NewMultiChan("c1:c2:c3")
  c3.NewMultiChan("c1:c2:c3")

  c1.Sub("c1:c2:c3")
  c2.Sub("c1:c2:c3")
  c3.Sub("c1:c2:c3")

  chat := func(c *pubsub.Client, which string) {
    defer wg.Done()
    if err := c.Pub(which + " says hi!", "c1:c2:c3"); err != nil {
      panic(err)
    }
    fmt.Println(which + " sent")
    c.DelChan("c1:c2:c3")

    name, msg, _, err := c.Recv(true)
    if err != nil {
      fmt.Printf("%s got an error: %v\n", which, err)
    } else {
      fmt.Printf("%s got a message on %s: %s\n", which, name, msg)
    }

    name, msg, _, err = c.Recv(true)
    if err != nil {
      fmt.Printf("%s got an error: %v\n", which, err)
    } else {
      fmt.Printf("%s got a message on %s: %s\n", which, name, msg)
    }

    name, msg, _, err = c.Recv(true)
    if err != nil {
      fmt.Printf("%s got an error: %v\n", which, err)
    } else {
      fmt.Printf("%s got a message on %s: %s\n", which, name, msg)
    }

    name, msg, _, err = c.Recv(true)
    if err != nil {
      fmt.Printf("%s got an error: %v\n", which, err)
    } else {
      fmt.Printf("%s got a message on %s: %s\n", which, name, msg)
    }

    name, msg, _, err = c.Recv(true)
    if err != nil {
      fmt.Printf("%s got an error: %v\n", which, err)
    } else {
      fmt.Printf("%s got a message on %s: %s\n", which, name, msg)
    }
  }

  wg.Add(1)
  go chat(c1, "c1")
  wg.Add(1)
  go chat(c2, "c2")
  wg.Add(1)
  go chat(c3, "c3")
  time.Sleep(time.Second * 3)

  println("waiting")
  wg.Wait()
}
