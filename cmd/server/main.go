package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"html/template"

	"github.com/localhots/koff"
)

type clusterState struct {
	consumerOffsets map[string]koff.OffsetMessage
	consumerGroups  map[string]koff.GroupMessage
}

var state = &clusterState{
	consumerOffsets: make(map[string]koff.OffsetMessage),
	consumerGroups:  make(map[string]koff.GroupMessage),
}

var lock sync.Mutex

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

	go func() {
		for msg := range c.Messages() {
			state.add(msg)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write(state.render())
	})
	http.ListenAndServe(":8080", nil)
}

func (s *clusterState) add(msg koff.Message) {
	lock.Lock()
	defer lock.Unlock()

	if msg.OffsetMessage != nil {
		cur, ok := s.consumerOffsets[msg.Consumer]
		if !ok || cur.CommittedAt.Before(msg.OffsetMessage.CommittedAt) {
			s.consumerOffsets[msg.Consumer] = *msg.OffsetMessage
		}
	} else {
		cur, ok := s.consumerGroups[msg.Consumer]
		if !ok || (msg.GroupMessage.GenerationID > cur.GenerationID && msg.GroupMessage.Complete()) {
			s.consumerGroups[msg.Consumer] = *msg.GroupMessage
		}
	}
}

//
// Render
//

var htmlTpl = template.Must(template.New("main").Parse(`<!doctype html>
<html>
<head>
<title>Kafka Consumers</title>
<meta http-equiv="refresh" content="3">
<style type="text/css">
* { font: 14px Helvetica; color: #222; }
h3 { font-size: 24px; font-weight: 600; margin-bottom: 15px; }
table { margin:0; padding:0; border:none; border-collapse:collapse; border-spacing:0; width: 100%; }
th, td { text-align: left; padding: 10px; }
th { border-bottom: #444 1px solid; font-weight: 600; }
th.numeric, td.numeric { text-align: right; }

table.gr { border: #444 1px solid; }
th.title { border: none; font-size: 16px; }
</style>
</head>

<body>

<h3>Consumer Offsets</h3>
<table>
<tr>
	<th width="40%">Consumer</th>
	<th width="35%">Topic</th>
	<th width="5%" class="numeric">Partition</th>
	<th width="10%" class="numeric">Offset</th>
	<th width="10%">Timestamp</th>
</tr>
{{range .ConsumerOffsets}}
<tr>
	<td>{{.Consumer}}</td>
	<td>{{.Topic}}</td>
	<td class="numeric">{{.Partition}}</td>
	<td class="numeric">{{.Offset}}</td>
	<td>{{.Timestamp}}</td>
</tr>
{{end}}
</table>
<br/>

<h3>Consumer Groups</h3>

{{range .ConsumerGroups}}
<table class="gr">
<tr>
	<th colspan="4" class="title">{{.Consumer}}</th>
</tr>
<tr>
	<th width="40%">Consumer</th>
	<th width="40%">Topic</th>
	<th width="10%" class="numeric">Partition</th>
	<th width="10%">Leader</th>
</tr>
{{range .Members}}
{{$id := .ID}}
{{range .Assignment}}
<tr>
	<td>{{$id}}</td>
	<td>{{.Topic}}</td>
	<td class="numeric">{{.Partition}}</td>
	<td>Yes</td>
</tr>
{{end}}
{{end}}
</table>
<br/>
{{end}}

</body>
</html>
`))

type offsetMessage struct {
	Consumer  string
	Timestamp string
	koff.OffsetMessage
}
type groupMessage struct {
	Consumer string
	koff.GroupMessage
}

func (s *clusterState) render() []byte {
	var tpl struct {
		ConsumerOffsets []offsetMessage
		ConsumerGroups  []groupMessage
	}

	lock.Lock()
	defer lock.Unlock()

	for k, m := range s.consumerOffsets {
		if strings.HasPrefix(k, "console-consumer") {
			continue
		}
		tpl.ConsumerOffsets = append(tpl.ConsumerOffsets, offsetMessage{
			Consumer:      k,
			OffsetMessage: m,
			Timestamp:     m.CommittedAt.Format(time.Stamp),
		})
	}
	sort.Slice(tpl.ConsumerOffsets, func(i, j int) bool {
		return tpl.ConsumerOffsets[i].Consumer < tpl.ConsumerOffsets[j].Consumer
	})

	for k, m := range s.consumerGroups {
		// if strings.HasPrefix(k, "console-consumer") {
		// 	continue
		// }
		tpl.ConsumerGroups = append(tpl.ConsumerGroups, groupMessage{
			Consumer:     k,
			GroupMessage: m,
		})
	}
	sort.Slice(tpl.ConsumerGroups, func(i, j int) bool {
		return tpl.ConsumerGroups[i].Consumer < tpl.ConsumerGroups[j].Consumer
	})

	var buf bytes.Buffer
	err := htmlTpl.Execute(&buf, tpl)
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}
