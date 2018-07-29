package koff

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/localhots/gobelt/log"
)

const topicName = "__consumer_offsets"

// Consumer reads messages from __consumer_offsets topic and decodes them into
// OffsetMessages.
type Consumer struct {
	client sarama.Client
	cons   sarama.Consumer
	pcs    map[int32]sarama.PartitionConsumer
	msgs   chan Message
	watch  bool

	lock    sync.Mutex
	wg      sync.WaitGroup
	pticker *time.Ticker
}

// NewConsumer creates a new Kafka offsets topic consumer.
func NewConsumer(brokers []string, watch bool) (*Consumer, error) {
	log.Info(context.TODO(), "Creating client")
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	log.Info(context.TODO(), "Creating consumer")
	sc, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	c := &Consumer{
		client: client,
		cons:   sc,
		pcs:    make(map[int32]sarama.PartitionConsumer),
		msgs:   make(chan Message),
		watch:  watch,
	}
	log.Info(context.TODO(), "Setting up partition consumers")
	if err := c.setupPartitionConsumers(); err != nil {
		return nil, err
	}

	if watch {
		c.wg.Add(1)
		go c.keepPartitionConsumersUpdated(30 * time.Second)
	}

	return c, nil
}

// Messages returns a read only channel of offset messages.
func (c *Consumer) Messages() <-chan Message {
	return c.msgs
}

// Close shuts down the consumer.
func (c *Consumer) Close() error {
	c.pticker.Stop()
	for _, pc := range c.pcs {
		if err := pc.Close(); err != nil {
			return err
		}
	}
	if err := c.cons.Close(); err != nil {
		return err
	}
	c.wg.Wait()
	close(c.msgs)
	return nil
}

func (c *Consumer) keepPartitionConsumersUpdated(interval time.Duration) {
	c.pticker = time.NewTicker(interval)
	for range c.pticker.C {
		c.setupPartitionConsumers()
	}
}

func (c *Consumer) setupPartitionConsumers() error {
	log.Info(context.TODO(), "Fetching partition list")
	partitions, err := c.cons.Partitions(topicName)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		if _, ok := c.pcs[partition]; !ok {
			c.wg.Add(1)
			go c.consumePartition(partition)
		}
	}

	pmap := make(map[int32]struct{}, len(partitions))
	for _, partition := range partitions {
		pmap[partition] = struct{}{}
	}

	for partition, pc := range c.pcs {
		if _, ok := pmap[partition]; !ok {
			err := pc.Close()
			if err != nil {
				return err
			}
			c.lock.Lock()
			delete(c.pcs, partition)
			c.lock.Unlock()
		}
	}

	return nil
}

func (c *Consumer) consumePartition(partition int32) error {
	defer c.wg.Done()

	pc, err := c.cons.ConsumePartition(topicName, partition, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	log.Info(context.TODO(), "Started partition consumer", log.F{"p": partition})

	c.lock.Lock()
	c.pcs[partition] = pc
	c.lock.Unlock()

	var maxOffset *int64
	if c.watch {
		off, err := c.client.GetOffset(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			return err
		}
		maxOffset = &off
	}

	for msg := range pc.Messages() {
		if msg.Value == nil {
			continue
		}
		c.msgs <- Decode(msg.Key, msg.Value)
		if maxOffset != nil && msg.Offset == *maxOffset {
			return nil
		}
	}

	return nil
}
