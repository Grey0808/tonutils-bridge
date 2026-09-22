package wsbridge

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stickyKey struct{}

// fakeNodes is a pool of liteservers numbered from 1, each answering a send
// the way answers says.
type fakeNodes struct {
	answers map[uint32]func() (tl.Serializable, error)

	mu    sync.Mutex
	asked []uint32
}

func (f *fakeNodes) QueryLiteserver(ctx context.Context, _ tl.Serializable, result tl.Serializable) error {
	id := f.StickyNodeID(ctx)
	f.mu.Lock()
	f.asked = append(f.asked, id)
	f.mu.Unlock()
	resp, err := f.answers[id]()
	if err != nil {
		return err
	}
	*(result.(*tl.Serializable)) = resp
	return nil
}

func (f *fakeNodes) StickyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, stickyKey{}, uint32(1))
}

func (f *fakeNodes) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	next := f.StickyNodeID(ctx) + 1
	if _, ok := f.answers[next]; !ok {
		return ctx, errors.New("no nodes left")
	}
	return context.WithValue(ctx, stickyKey{}, next), nil
}

func (f *fakeNodes) StickyContextNextNodeBalanced(ctx context.Context) (context.Context, error) {
	return f.StickyContextNextNode(ctx)
}

func (f *fakeNodes) StickyNodeID(ctx context.Context) uint32 {
	id, _ := ctx.Value(stickyKey{}).(uint32)
	return id
}

// fakeAPI is an API client whose only working part is the pool behind it.
type fakeAPI struct {
	ton.APIClientWrapped
	nodes *fakeNodes
}

func (f fakeAPI) Client() ton.LiteClient { return f.nodes }

func sendAll(t *testing.T, nodes *fakeNodes) WSResponse {
	t.Helper()
	cfg := DefaultBridgeConfig()
	cfg.AllowedOrigins = []string{"*"}
	bridge := NewWSBridge(cfg, nil, fakeAPI{nodes: nodes}, nil, nil, nil)
	conn, cleanup := dialTestBridge(t, bridge)
	t.Cleanup(cleanup)
	boc := base64.StdEncoding.EncodeToString(cell.BeginCell().MustStoreUInt(7, 32).EndCell().ToBOC())
	return rpc(t, conn, "1", "lite.sendMessageAll", map[string]string{"boc": boc})
}

func accepted() (tl.Serializable, error) { return ton.SendMessageStatus{Status: 1}, nil }
func refused() (tl.Serializable, error) {
	return ton.LSError{Code: 0, Text: "cannot apply external message to current state"}, nil
}
func timedOut() (tl.Serializable, error) { return nil, errors.New("adnl request timeout") }

func sendAllResult(t *testing.T, resp WSResponse) map[string]any {
	t.Helper()
	if resp.Error != nil {
		t.Fatalf("error %+v", resp.Error)
	}
	raw, _ := json.Marshal(resp.Result)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSendMessageAll_AcceptedByOneIsAccepted(t *testing.T) {
	nodes := &fakeNodes{answers: map[uint32]func() (tl.Serializable, error){1: timedOut, 2: refused, 3: accepted}}
	out := sendAllResult(t, sendAll(t, nodes))
	if out["status"] != float64(1) || out["asked"] != float64(3) {
		t.Fatalf("answered %v", out)
	}
}

func TestSendMessageAll_RefusedByAllIsStatusZeroWithTheReason(t *testing.T) {
	nodes := &fakeNodes{answers: map[uint32]func() (tl.Serializable, error){1: refused, 2: timedOut}}
	out := sendAllResult(t, sendAll(t, nodes))
	if out["status"] != float64(0) || out["error"] == nil {
		t.Fatalf("answered %v", out)
	}
	if len(out["nodes"].([]any)) != 2 {
		t.Fatalf("nodes %v", out["nodes"])
	}
}

func TestSendMessageAll_NoAnswerAtAllIsAnError(t *testing.T) {
	nodes := &fakeNodes{answers: map[uint32]func() (tl.Serializable, error){1: timedOut, 2: timedOut}}
	if resp := sendAll(t, nodes); resp.Error == nil {
		t.Fatalf("answered %v with no liteserver answering", resp.Result)
	}
}

func TestSendMessageAll_AsksEveryConnectedNode(t *testing.T) {
	nodes := &fakeNodes{answers: map[uint32]func() (tl.Serializable, error){1: refused, 2: refused, 3: refused, 4: refused}}
	sendAllResult(t, sendAll(t, nodes))
	nodes.mu.Lock()
	defer nodes.mu.Unlock()
	if len(nodes.asked) != 4 {
		t.Fatalf("asked %v", nodes.asked)
	}
}
