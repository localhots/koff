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

	ctx     context.Context
	stopFn  func()
	lock    sync.Mutex
	wg      sync.WaitGroup
	pticker *time.Ticker
}

// NewConsumer creates a new Kafka offsets topic consumer.
func NewConsumer(brokers []string, watch bool) (*Consumer, error) {
	ctx := context.Background()
	log.Info(ctx, "Creating client", log.F{"brokers": brokers})
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}

	log.Info(ctx, "Creating consumer")
	sc, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Consumer{
		client: client,
		cons:   sc,
		pcs:    make(map[int32]sarama.PartitionConsumer),
		msgs:   make(chan Message),
		watch:  watch,
		ctx:    ctx,
		stopFn: cancel,
	}
	log.Info(ctx, "Setting up partition consumers")
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
	c.stopFn()
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
	for {
		select {
		case <-c.pticker.C:
			if err := c.setupPartitionConsumers(); err != nil {
				log.Error(c.ctx, "Failed to setup update partition consumers", log.F{"error": err})
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Consumer) setupPartitionConsumers() error {
	log.Info(c.ctx, "Fetching partition list")
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
	ctx := log.ContextWithFields(c.ctx, log.F{"partition": partition})

	pc, err := c.cons.ConsumePartition(topicName, partition, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	c.lock.Lock()
	c.pcs[partition] = pc
	c.lock.Unlock()

	var maxOffset *int64
	if c.watch {
		log.Debug(ctx, "Fetching last offset")
		off, err := c.client.GetOffset(topicName, partition, sarama.OffsetNewest)
		if err != nil {
			return err
		}
		maxOffset = &off
	}

	log.Info(ctx, "Started partition consumer")
	for msg := range pc.Messages() {
		if msg.Value == nil {
			continue
		}
		ctx := log.ContextWithFields(ctx, log.F{"offset": msg.Offset})
		c.msgs <- Decode(ctx, msg.Key, msg.Value)
		if maxOffset != nil && msg.Offset == *maxOffset {
			return nil
		}
	}

	return nil
}
