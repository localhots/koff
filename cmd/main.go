package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/localhots/koff"
	"github.com/localhots/pretty"
)

const topicName = "__consumer_offsets"

func main() {
	brokers := flag.String("brokers", "", "Comma separated list of brokers")
	flag.Parse()

	if *brokers == "" {
		fmt.Println("Brokers list required")
		flag.Usage()
		os.Exit(1)
	}

	c, err := koff.NewConsumer(strings.Split(*brokers, ","), false)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer c.Close()

	for msg := range c.Messages() {
		if msg.GroupMessage != nil {
			pretty.Println("MESSAGE", msg)
		}
	}
}
