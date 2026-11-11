// Copyright IBM Corp. 2024, 2026
// SPDX-License-Identifier: BUSL-1.1

package submatview_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hashicorp/consul/agent/rpcclient"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/grpc-internal/services/subscribe"
	"github.com/hashicorp/consul/agent/rpcclient/health"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/submatview"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestStore_IntegrationWithBackend(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	var maxIndex uint64 = 200
	count := &counter{latest: 3}
	producers := map[string]*eventProducer{
		state.EventSubjectService{Key: "srv1"}.String(): newEventProducer(pbsubscribe.Topic_ServiceHealth, "srv1", count, maxIndex),
		state.EventSubjectService{Key: "srv2"}.String(): newEventProducer(pbsubscribe.Topic_ServiceHealth, "srv2", count, maxIndex),
		state.EventSubjectService{Key: "srv3"}.String(): newEventProducer(pbsubscribe.Topic_ServiceHealth, "srv3", count, maxIndex),
	}

	sh := snapshotHandler{producers: producers}
	pub := stream.NewEventPublisher(10 * time.Millisecond)
	pub.RegisterHandler(pbsubscribe.Topic_ServiceHealth, sh.Snapshot, false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	store := submatview.NewStore(hclog.New(nil))
	go store.Run(ctx)

	addr := runServer(t, pub)

	consumers := []*consumer{
		newConsumer(t, addr, store, "srv1"),
		newConsumer(t, addr, store, "srv1"),
		newConsumer(t, addr, store, "srv1"),
		newConsumer(t, addr, store, "srv2"),
		newConsumer(t, addr, store, "srv2"),
		newConsumer(t, addr, store, "srv2"),
	}

	group, gctx := errgroup.WithContext(ctx)
	for i := range producers {
		producer := producers[i]
		group.Go(func() error {
			producer.Produce(gctx, pub)
			return nil
		})
	}

	for i := range consumers {
		consumer := consumers[i]
		group.Go(func() error {
			return consumer.Consume(gctx, maxIndex)
		})
	}

	_ = group.Wait()

	for i, consumer := range consumers {
		t.Run(fmt.Sprintf("consumer %d", i), func(t *testing.T) {
			require.True(t, len(consumer.states) > 2, "expected more than %d events", len(consumer.states))

			expected := producers[state.EventSubjectService{Key: consumer.srvName}.String()].nodesByIndex
			for idx, nodes := range consumer.states {
				assertDeepEqual(t, idx, expected[idx], nodes)
			}
		})
	}
}

func assertDeepEqual(t *testing.T, idx uint64, x, y interface{}) {
	t.Helper()
	if diff := cmp.Diff(x, y, cmpopts.EquateEmpty()); diff != "" {
		t.Fatalf("assertion failed: values at index %d are not equal\n--- expected\n+++ actual\n%v", idx, diff)
	}
}

func stateFromUpdates(u cache.UpdateEvent) []string {
	var result []string
	for _, node := range u.Result.(*structs.IndexedCheckServiceNodes).Nodes {
		result = append(result, node.Node.Node)
	}

	sort.Strings(result)
	return result
}

func runServer(t *testing.T, pub *stream.EventPublisher) net.Addr {
	subSrv := &subscribe.Server{
		Backend: backend{pub: pub},
		Logger:  hclog.New(nil),
	}
	srv := grpc.NewServer()
	pbsubscribe.RegisterStateChangeSubscriptionServer(srv, subSrv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var g errgroup.Group
	g.Go(func() error {
		return srv.Serve(lis)
	})
	t.Cleanup(func() {
		srv.Stop()
		if err := g.Wait(); err != nil {
			t.Log(err.Error())
		}
	})

	return lis.Addr()
}

type backend struct {
	pub *stream.EventPublisher
}

func (b backend) ResolveTokenAndDefaultMeta(string, *acl.EnterpriseMeta, *acl.AuthorizerContext) (acl.Authorizer, error) {
	return acl.AllowAll(), nil
}

func (b backend) Forward(structs.RPCInfo, func(*grpc.ClientConn) error) (handled bool, err error) {
	return false, nil
}

func (b backend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	return b.pub.Subscribe(req)
}

var _ subscribe.Backend = (*backend)(nil)

type eventProducer struct {
	rand         *rand.Rand
	counter      *counter
	topic        stream.Topic
	srvName      string
	nodesByIndex map[uint64][]string
	nodesLock    sync.Mutex
	maxIndex     uint64
}

func newEventProducer(
	topic stream.Topic,
	srvName string,
	counter *counter,
	maxIndex uint64,
) *eventProducer {
	return &eventProducer{
		rand:         rand.New(rand.NewSource(time.Now().UnixNano())),
		counter:      counter,
		nodesByIndex: map[uint64][]string{},
		topic:        topic,
		srvName:      srvName,
		maxIndex:     maxIndex,
	}
}

var minEventDelay = 5 * time.Millisecond

func (e *eventProducer) Produce(ctx context.Context, pub *stream.EventPublisher) {
	var nodes []string
	var nextID int

	for ctx.Err() == nil {
		var event stream.Event

		action := e.rand.Intn(3)
		if len(nodes) == 0 {
			action = 1
		}

		idx := e.counter.Next()
		switch action {

		case 0: // Deregister
			nodeIdx := e.rand.Intn(len(nodes))
			node := nodes[nodeIdx]
			nodes = append(nodes[:nodeIdx], nodes[nodeIdx+1:]...)

			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Deregister,
					Value: &structs.CheckServiceNode{
						Node: &structs.Node{Node: node},
						Service: &structs.NodeService{
							ID:      e.srvName,
							Service: e.srvName,
						},
					},
				},
			}

		case 1: // Register new
			node := nodeName(nextID)
			nodes = append(nodes, node)
			nextID++

			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Register,
					Value: &structs.CheckServiceNode{
						Node: &structs.Node{Node: node},
						Service: &structs.NodeService{
							ID:      e.srvName,
							Service: e.srvName,
						},
					},
				},
			}

		case 2: // Register update
			node := nodes[e.rand.Intn(len(nodes))]
			event = stream.Event{
				Topic: e.topic,
				Index: idx,
				Payload: state.EventPayloadCheckServiceNode{
					Op: pbsubscribe.CatalogOp_Register,
					Value: &structs.CheckServiceNode{
						Node: &structs.Node{Node: node},
						Service: &structs.NodeService{
							ID:      e.srvName,
							Service: e.srvName,
						},
					},
				},
			}
		}

		e.nodesLock.Lock()
		pub.Publish([]stream.Event{event})
		e.nodesByIndex[idx] = copyNodeList(nodes)
		e.nodesLock.Unlock()

		if idx > e.maxIndex {
			return
		}

		delay := time.Duration(rand.Intn(25)) * time.Millisecond
		time.Sleep(minEventDelay + delay)
	}
}

func nodeName(i int) string {
	return fmt.Sprintf("node-%d", i)
}

func copyNodeList(nodes []string) []string {
	result := make([]string, len(nodes))
	copy(result, nodes)
	sort.Strings(result)
	return result
}

type counter struct {
	latest uint64
}

func (c *counter) Next() uint64 {
	return atomic.AddUint64(&c.latest, 1)
}

type consumer struct {
	healthClient *health.Client
	states       map[uint64][]string
	srvName      string
}

func newConsumer(t *testing.T, addr net.Addr, store *submatview.Store, srv string) *consumer {
	//nolint:staticcheck
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	require.NoError(t, err)

	c := &health.Client{
		Client: rpcclient.Client{
			UseStreamingBackend: true,
			ViewStore:           store,
			MaterializerDeps: rpcclient.MaterializerDeps{
				Conn:   conn,
				Logger: hclog.New(nil),
			},
		},
	}

	return &consumer{
		healthClient: c,
		states:       make(map[uint64][]string),
		srvName:      srv,
	}
}

func (c *consumer) Consume(ctx context.Context, maxIndex uint64) error {
	req := structs.ServiceSpecificRequest{ServiceName: c.srvName}
	updateCh := make(chan cache.UpdateEvent, 10)

	group, cctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return c.healthClient.Notify(cctx, req, "", func(ctx context.Context, event cache.UpdateEvent) {
			select {
			case updateCh <- event:
			case <-ctx.Done():
			}
		})
	})
	group.Go(func() error {
		var idx uint64
		for {
			if idx >= maxIndex {
				return nil
			}
			select {
			case u := <-updateCh:
				idx = u.Meta.Index
				c.states[u.Meta.Index] = stateFromUpdates(u)
			case <-cctx.Done():
				return nil
			}
		}
	})
	return group.Wait()
}

type snapshotHandler struct {
	producers map[string]*eventProducer
}

func (s *snapshotHandler) Snapshot(req stream.SubscribeRequest, buf stream.SnapshotAppender) (index uint64, err error) {
	producer := s.producers[req.Subject.String()]

	producer.nodesLock.Lock()
	defer producer.nodesLock.Unlock()
	idx := atomic.LoadUint64(&producer.counter.latest)

	// look backwards for an index that was used by the producer
	nodes, ok := producer.nodesByIndex[idx]
	for !ok && idx > 0 {
		idx--
		nodes, ok = producer.nodesByIndex[idx]
	}

	for _, node := range nodes {
		event := stream.Event{
			Topic: producer.topic,
			Index: idx,
			Payload: state.EventPayloadCheckServiceNode{
				Op: pbsubscribe.CatalogOp_Register,
				Value: &structs.CheckServiceNode{
					Node: &structs.Node{Node: node},
					Service: &structs.NodeService{
						ID:      producer.srvName,
						Service: producer.srvName,
					},
				},
			},
		}
		buf.Append([]stream.Event{event})
	}
	return idx, nil
}

// TestStore_IntegrationWithBackend_ServerStop stops the server of a stream
// with a hard Stop, as consul.Server.Shutdown does. While another server is
// up, a blocked query must not see an error, and it must get the next update.
// When no server is up, the blocked query must get the error.
func TestStore_IntegrationWithBackend_ServerStop(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sh := &staticSnapshotHandler{srvName: "srv1"}
	pub := stream.NewEventPublisher(10 * time.Millisecond)
	pub.RegisterHandler(pbsubscribe.Topic_ServiceHealth, sh.Snapshot, false)
	go pub.Run(ctx)
	sh.register(pub, 10, "node-a")

	store := submatview.NewStore(hclog.New(nil))
	go store.Run(ctx)

	// Both servers share the publisher, like servers that share the raft log.
	// pick_first uses the first address, so the stream goes to srvA.
	srvA, addrA := startStoppableServer(t, pub)
	srvB, addrB := startStoppableServer(t, pub)

	rb := manual.NewBuilderWithScheme("stop-test")
	rb.InitialState(resolver.State{Addresses: []resolver.Address{
		{Addr: addrA.String()},
		{Addr: addrB.String()},
	}})
	conn, err := grpc.NewClient(rb.Scheme()+":///servers",
		grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := &health.Client{
		Client: rpcclient.Client{
			UseStreamingBackend: true,
			ViewStore:           store,
			MaterializerDeps: rpcclient.MaterializerDeps{
				Conn:   conn,
				Logger: hclog.New(nil),
			},
			QueryOptionDefaults: func(*structs.QueryOptions) {},
		},
	}

	req := structs.ServiceSpecificRequest{
		ServiceName:  "srv1",
		QueryOptions: structs.QueryOptions{UseCache: true, MaxQueryTime: 5 * time.Second},
	}
	out, meta, err := client.ServiceNodes(ctx, req)
	require.NoError(t, err)
	require.Equal(t, uint64(10), meta.Index)
	require.Equal(t, []string{"node-a"}, nodeNames(out))

	type result struct {
		out  structs.IndexedCheckServiceNodes
		meta cache.ResultMeta
		err  error
	}
	blockingQuery := func(minIndex uint64) <-chan result {
		ch := make(chan result, 1)
		go func() {
			r := req
			r.MinQueryIndex = minIndex
			out, meta, err := client.ServiceNodes(ctx, r)
			ch <- result{out, meta, err}
		}()
		return ch
	}

	t.Run("stopped server, another server up", func(t *testing.T) {
		ch := blockingQuery(10)
		time.Sleep(200 * time.Millisecond)
		srvA.Stop()

		// Give the materializer time to see the stop and resubscribe before
		// the next event, so the event comes over the new stream.
		select {
		case r := <-ch:
			t.Fatalf("blocked query returned after the server stop: index=%d err=%v", r.meta.Index, r.err)
		case <-time.After(time.Second):
		}

		sh.register(pub, 11, "node-b")
		select {
		case r := <-ch:
			require.NoError(t, r.err)
			require.Equal(t, uint64(11), r.meta.Index)
			require.Equal(t, []string{"node-a", "node-b"}, nodeNames(r.out))
		case <-time.After(4 * time.Second):
			t.Fatal("blocked query did not get the update from the other server")
		}
	})

	t.Run("no server up", func(t *testing.T) {
		ch := blockingQuery(11)
		time.Sleep(200 * time.Millisecond)
		start := time.Now()
		srvB.Stop()

		select {
		case r := <-ch:
			require.Error(t, r.err)
			t.Logf("blocked query got the error after %s: %v", time.Since(start), r.err)
		case <-time.After(4 * time.Second):
			t.Fatal("blocked query did not get an error with no server up")
		}
	})
}

func startStoppableServer(t *testing.T, pub *stream.EventPublisher) (*grpc.Server, net.Addr) {
	srv := grpc.NewServer()
	pbsubscribe.RegisterStateChangeSubscriptionServer(srv, &subscribe.Server{
		Backend: backend{pub: pub},
		Logger:  hclog.New(nil),
	})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return srv, lis.Addr()
}

func nodeNames(out structs.IndexedCheckServiceNodes) []string {
	var names []string
	for _, n := range out.Nodes {
		names = append(names, n.Node.Node)
	}
	sort.Strings(names)
	return names
}

// staticSnapshotHandler holds the current nodes of one service, so that a
// snapshot on any server returns the latest state.
type staticSnapshotHandler struct {
	srvName string
	lock    sync.Mutex
	index   uint64
	nodes   []string
}

func (s *staticSnapshotHandler) register(pub *stream.EventPublisher, index uint64, node string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.index = index
	s.nodes = append(s.nodes, node)
	pub.Publish([]stream.Event{s.event(index, node)})
}

func (s *staticSnapshotHandler) event(index uint64, node string) stream.Event {
	return stream.Event{
		Topic: pbsubscribe.Topic_ServiceHealth,
		Index: index,
		Payload: state.EventPayloadCheckServiceNode{
			Op: pbsubscribe.CatalogOp_Register,
			Value: &structs.CheckServiceNode{
				Node:    &structs.Node{Node: node},
				Service: &structs.NodeService{ID: s.srvName, Service: s.srvName},
			},
		},
	}
}

func (s *staticSnapshotHandler) Snapshot(_ stream.SubscribeRequest, buf stream.SnapshotAppender) (uint64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, node := range s.nodes {
		buf.Append([]stream.Event{s.event(s.index, node)})
	}
	return s.index, nil
}
