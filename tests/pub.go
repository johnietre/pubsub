package main

import (
  "strconv"
  "time"

  "github.com/johnietre/pubsub/client"
)

var dur = time.Second * 5

func main() {
  client, err := client.NewClient("127.0.0.1:8000", 5, false); must(err)
  defer client.Close()
  err = client.NewChan("chan3", "chan5", "chan15", "chann"); must(err)
  chann, ok := client.GetChan("chann")
  if !ok {
    panic("channel not loaded")
  }
  time.Sleep(dur)
  client.Pub("start", "chan3", "chan5", "chan15", "chann")
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
  time.Sleep(time.Second * 10)
}

func must(err error) {
  if err != nil {
    panic(err)
  }
}
