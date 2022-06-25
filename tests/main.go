package main

import (
  "fmt"
  "reflect"
  "time"
)

func main() {
  c := make(chan int, 1)
  var chans []reflect.SelectCase
  for i := 0; i < 20; i++ {
    chans = append(chans, reflect.SelectCase{Chan: reflect.ValueOf(make(chan int)), Dir: reflect.SelectRecv})
  }
  chans = append(chans, reflect.SelectCase{Chan: reflect.ValueOf(c), Dir: reflect.SelectRecv})

  go func() {
    time.Sleep(time.Second * 10)
    close(c)
    fmt.Println("Done")
  }()

  for {
    fmt.Println(reflect.Select(chans))
    return
  }
}
