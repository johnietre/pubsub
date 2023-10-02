package main

import (
	"fmt"
	"github.com/johnietre/pubsub"
)

func main() {
	client, err := pubsub.NewClient("127.0.0.1:8000", 100, false)
	must(err)
	defer client.Close()
	client.Sub("chan3", "chan5", "chan15", "chann")

	name, msg, _, err := client.Recv(true)
	if err != nil {
		fmt.Printf("error with %s: %v", name, err)
		return
	}
	fmt.Printf("%s: %s\n", name, msg)

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
