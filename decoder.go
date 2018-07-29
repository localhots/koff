package koff

import (
	"context"
	"encoding/binary"
	"runtime/debug"
	"time"

	"github.com/localhots/gobelt/log"
)

// Message is the main structure that wraps a consumer offsets topic message.
type Message struct {
	Consumer      string
	OffsetMessage *OffsetMessage
	GroupMessage  *GroupMessage
}

// OffsetMessage is a kind of message that carries individual consumer offset.
type OffsetMessage struct {
	Topic       string
	Partition   int32
	Offset      int64
	Metadata    string
	CommittedAt time.Time
	ExpiresAt   time.Time
}

// GroupMessage contains consumer group metadata.
type GroupMessage struct {
	ProtocolType string
	GenerationID int32
	LeaderID     string
	Protocol     string
	Members      []GroupMember
}

// GroupMember contains metadata for a consumer group member.
type GroupMember struct {
	ID               string
	ClientID         string
	ClientHost       string
	SessionTimeout   time.Duration
	RebalanceTimeout time.Duration
	Subscription     []TopicAndPartition
	Assignment       []TopicAndPartition
}

// TopicAndPartition is a tuple of topic and partition.
type TopicAndPartition struct {
	Topic     string
	Partition int32
}

// Decode decodes message key and value into an OffsetMessage.
func Decode(ctx context.Context, key, val []byte) Message {
	var m Message
	defer func() {
		if err := recover(); err != nil {
			log.Error(ctx, "Failed to decode group metadata", log.F{
				"error":   err,
				"payload": val,
				"message": m,
			})
			debug.PrintStack()
		}
	}()

	m.decodeKey(key)
	if m.OffsetMessage != nil {
		m.decodeOffsetMessage(val)
	} else {
		m.decodeGroupMetadata(ctx, val)
	}

	return m
}

// Key structure:
// [2] Version, uint16 big endian
// [2] Consumer name length, uint16 big endian
// [^] Consumer name
// if Version is 0 or 1 {
//     [2] Topic length, uint16 big endian
//     [^] Topic
//     [4] Partition, uint32 big endian
// }
func (m *Message) decodeKey(key []byte) {
	buf := &buffer{data: key}
	version := buf.readInt16()
	m.Consumer = buf.readString()
	if version < 2 {
		m.OffsetMessage = &OffsetMessage{
			Topic:     buf.readString(),
			Partition: buf.readInt32(),
		}
	}
}

// Value structure:
// [2] Version, uint16 big endian
// [8] Offset, uint32 big endian
// [2] Meta length, uint16 big endian
// [^] Meta
// [8] Commit time, unix timestamp with millisecond precision
// if Version is 1 {
//     [8] Expire time, unix timestamp with millisecond precision
// }
func (m *Message) decodeOffsetMessage(val []byte) {
	buf := &buffer{data: val}
	om := m.OffsetMessage
	version := buf.readInt16()
	om.Offset = buf.readInt64()
	om.Metadata = buf.readString()
	om.CommittedAt = makeTime(buf.readInt64())
	if version == 1 {
		om.ExpiresAt = makeTime(buf.readInt64())
	}
}

func (m *Message) decodeGroupMetadata(ctx context.Context, val []byte) {
	buf := &buffer{data: val}
	m.GroupMessage = &GroupMessage{Members: []GroupMember{}}
	gm := m.GroupMessage
	version := buf.readInt16()
	if version > 1 {
		return
	}
	gm.ProtocolType = buf.readString()
	gm.GenerationID = buf.readInt32()
	if gm.GenerationID > 1 {
		// Messages with generation greater than one often lack remaining fields
		// If followin sequence is like 0xFF 0xFF 0xFF 0xFF 0x00 0x00 ...
		// then just skip that shit
		next := buf.readInt32()
		if next == -1 {
			return
		}
		buf.pos -= 4
	}

	gm.LeaderID = buf.readString()
	gm.Protocol = buf.readString()

	arySize := int(buf.readInt32())
	for i := 0; i < arySize; i++ {
		gm.Members = append(gm.Members, GroupMember{
			ID:               buf.readString(),
			ClientID:         buf.readString(),
			ClientHost:       buf.readString(),
			SessionTimeout:   makeDur(buf.readInt32()),
			RebalanceTimeout: makeDur(buf.readInt32()),
			Subscription:     readAssignment(buf),
			Assignment:       readAssignment(buf),
		})
	}
}

func readAssignment(buf *buffer) []TopicAndPartition {
	ass := []TopicAndPartition{}
	buf.skip(2) // Eh
	size := buf.readInt32()
	if size == 0 {
		buf.skip(4)
		return ass
	}
	for i := 0; i < int(size); i++ {
		ass = append(ass, readTopicAndSubscription(buf))
		buf.skip(4) // Hash key or smth
	}
	return ass
}

func readTopicAndSubscription(buf *buffer) TopicAndPartition {
	return TopicAndPartition{
		Topic:     buf.readString(),
		Partition: buf.readInt32(),
	}
}

func makeTime(ts int64) time.Time {
	return time.Unix(ts/1000, (ts%1000)*1000000)
}

func makeDur(to int32) time.Duration {
	return time.Duration(to) * time.Millisecond
}

//
// Buffer
//

type buffer struct {
	data []byte
	pos  int
}

func (b *buffer) skip(n int) {
	b.pos += n
}

func (b *buffer) readBytes(n int) []byte {
	b.pos += n
	return b.data[b.pos-n : b.pos]
}

func (b *buffer) readInt16() int16 {
	i := binary.BigEndian.Uint16(b.data[b.pos:])
	b.pos += 2
	return int16(i)
}

func (b *buffer) readInt32() int32 {
	i := binary.BigEndian.Uint32(b.data[b.pos:])
	b.pos += 4
	return int32(i)
}

func (b *buffer) readInt64() int64 {
	i := binary.BigEndian.Uint64(b.data[b.pos:])
	b.pos += 8
	return int64(i)
}

func (b *buffer) readString() string {
	strlen := int(b.readInt16())
	b.pos += strlen
	return string(b.data[b.pos-strlen : b.pos])
}
