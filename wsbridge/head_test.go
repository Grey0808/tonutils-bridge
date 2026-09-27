package wsbridge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// pinKey is where numberedNodes keeps a pin, as tonutils-go keeps its own under a
// key of its own.
type pinKey struct{}

// numberedNodes is a pool of liteservers numbered 1 to n.
type numberedNodes struct{ n uint32 }

func (f numberedNodes) QueryLiteserver(context.Context, tl.Serializable, tl.Serializable) error {
	return errors.New("not a liteserver")
}

func (f numberedNodes) StickyContext(ctx context.Context) context.Context {
	if f.StickyNodeID(ctx) != 0 {
		return ctx
	}
	return context.WithValue(ctx, pinKey{}, uint32(1))
}

func (f numberedNodes) StickyContextNextNode(ctx context.Context) (context.Context, error) {
	id := f.StickyNodeID(ctx)
	if id >= f.n {
		return ctx, errors.New("no nodes left")
	}
	return context.WithValue(ctx, pinKey{}, id+1), nil
}

func (f numberedNodes) StickyContextNextNodeBalanced(ctx context.Context) (context.Context, error) {
	return f.StickyContextNextNode(ctx)
}

func (f numberedNodes) StickyNodeID(ctx context.Context) uint32 {
	id, _ := ctx.Value(pinKey{}).(uint32)
	return id
}

// headAPI is one liteserver whose chain grows a block each time it is asked
// to wait for the next one, and whose cached head never moves.
type headAPI struct {
	ton.APIClientWrapped
	tip     atomic.Uint32
	waitFor atomic.Uint32
}

func (a *headAPI) Client() ton.LiteClient { return numberedNodes{n: 1} }

func (a *headAPI) GetMasterchainInfo(context.Context) (*ton.BlockIDExt, error) {
	if w := a.waitFor.Load(); w > a.tip.Load() {
		a.tip.Store(w)
	}
	return &ton.BlockIDExt{Workchain: -1, SeqNo: a.tip.Load()}, nil
}

func (a *headAPI) CurrentMasterchainInfo(context.Context) (*ton.BlockIDExt, error) {
	return &ton.BlockIDExt{Workchain: -1, SeqNo: 100}, nil
}

func (a *headAPI) WaitForBlock(seqno uint32) ton.APIClientWrapped {
	a.waitFor.Store(seqno)
	return a
}

// chainsAPI is a pool of liteservers each with a chain of its own: a wait on
// one for a block it does not have returns only once the test gives it one.
type chainsAPI struct {
	ton.APIClientWrapped
	nodes numberedNodes
	mu    sync.Mutex
	tips  map[uint32]uint32
}

func (a *chainsAPI) Client() ton.LiteClient { return a.nodes }

func (a *chainsAPI) setTip(node, seqno uint32) {
	a.mu.Lock()
	a.tips[node] = seqno
	a.mu.Unlock()
}

func (a *chainsAPI) tipOn(ctx context.Context) uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.tips[a.nodes.StickyNodeID(ctx)]
}

func (a *chainsAPI) GetMasterchainInfo(ctx context.Context) (*ton.BlockIDExt, error) {
	return &ton.BlockIDExt{Workchain: -1, SeqNo: a.tipOn(ctx)}, nil
}

func (a *chainsAPI) WaitForBlock(seqno uint32) ton.APIClientWrapped {
	return &waitingAPI{chainsAPI: a, seqno: seqno}
}

type waitingAPI struct {
	*chainsAPI
	seqno uint32
}

func (w *waitingAPI) GetMasterchainInfo(ctx context.Context) (*ton.BlockIDExt, error) {
	for w.tipOn(ctx) < w.seqno {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return w.chainsAPI.GetMasterchainInfo(ctx)
}

func TestAStateIsReadAtTheNewestBlockFollowedNotTheSecondOldHead(t *testing.T) {
	api := &headAPI{}
	api.tip.Store(100)
	b := &WSBridge{api: api}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.followHead(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, blk, _, err := b.stateHead(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if blk.SeqNo > 105 {
			if api.waitFor.Load() < blk.SeqNo {
				t.Fatalf("read at %d without asking the liteserver to wait for it", blk.SeqNo)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("still reading at %d, the cached head", blk.SeqNo)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTheHeadIsTheNewestBlockAnyLiteserverHasNamedWhileAnotherLags(t *testing.T) {
	api := &chainsAPI{nodes: numberedNodes{n: 3}, tips: map[uint32]uint32{1: 100, 2: 100, 3: 100}}
	b := &WSBridge{api: api}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.followHead(ctx)
	waitFor := func(seqno uint32) *followedHead {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if h := b.newest.Load(); h != nil && h.block.SeqNo == seqno {
				return h
			}
			if time.Now().After(deadline) {
				t.Fatalf("the head never reached %d", seqno)
			}
			time.Sleep(time.Millisecond)
		}
	}
	waitFor(100)
	api.setTip(2, 101)
	h := waitFor(101)
	if len(h.nodes) != 1 || h.nodes[0].id != 2 {
		t.Fatalf("101 is credited to %v, not to liteserver 2 alone", h.nodes)
	}
	readCtx, _, _, err := b.stateHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := api.nodes.StickyNodeID(readCtx); got != 2 {
		t.Fatalf("a read at 101 goes to liteserver %d, which has not named it", got)
	}
}

func TestAStateIsReadOnlyOnTheLiteserversThatHaveNamedTheBlockTakenInTurn(t *testing.T) {
	nodes := numberedNodes{n: 3}
	b := &WSBridge{api: &headAPI{}}
	pin := func(id uint32) headNode {
		return headNode{id: id, pin: context.WithValue(context.Background(), pinKey{}, id)}
	}
	b.noteHead(&ton.BlockIDExt{SeqNo: 101}, pin(2))
	b.noteHead(&ton.BlockIDExt{SeqNo: 100}, pin(1))
	b.noteHead(&ton.BlockIDExt{SeqNo: 101}, pin(3))
	b.noteHead(&ton.BlockIDExt{SeqNo: 101}, pin(2))
	seen := map[uint32]int{}
	for i := 0; i < 10; i++ {
		ctx, blk, _, _ := b.stateHead(context.Background())
		if blk.SeqNo != 101 {
			t.Fatalf("read at %d", blk.SeqNo)
		}
		seen[nodes.StickyNodeID(ctx)]++
	}
	if seen[2] != 5 || seen[3] != 5 {
		t.Fatalf("reads went to %v, not to 2 and 3 in turn", seen)
	}
}

func TestAPinnedContextKeepsItsOwnDeadlineCancellationAndValues(t *testing.T) {
	type mine struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), mine{}, "v"), time.Hour)
	p := pinned{parent, context.WithValue(context.Background(), pinKey{}, uint32(7))}
	if d, ok := p.Deadline(); !ok || time.Until(d) < 59*time.Minute {
		t.Fatalf("deadline %v %v", d, ok)
	}
	if p.Value(mine{}) != "v" || (numberedNodes{}).StickyNodeID(p) != 7 {
		t.Fatalf("values: %v %v", p.Value(mine{}), p.Value(pinKey{}))
	}
	child, stop := context.WithTimeout(p, time.Hour)
	defer stop()
	cancel()
	select {
	case <-child.Done():
	case <-time.After(time.Second):
		t.Fatal("a context made from a pinned one outlived the one it was pinned from")
	}
}

func TestWithNoFollowedHeadAStateIsReadAtTheCachedOne(t *testing.T) {
	b := &WSBridge{api: &headAPI{}}
	_, blk, _, err := b.stateHead(context.Background())
	if err != nil || blk.SeqNo != 100 {
		t.Fatalf("%v %v", blk, err)
	}
	b.newest.Store(&followedHead{block: &ton.BlockIDExt{SeqNo: 200}, at: time.Now().Add(-time.Minute)})
	if _, blk, _, _ := b.stateHead(context.Background()); blk.SeqNo != 100 {
		t.Fatalf("a followed head a minute old was trusted: %d", blk.SeqNo)
	}
}

func TestAnOlderBlockNeverReplacesTheNewestNoted(t *testing.T) {
	b := &WSBridge{}
	b.noteHead(&ton.BlockIDExt{SeqNo: 10}, headNode{id: 1, pin: context.Background()})
	b.noteHead(&ton.BlockIDExt{SeqNo: 9}, headNode{id: 2, pin: context.Background()})
	if h := b.newest.Load(); h.block.SeqNo != 10 || len(h.nodes) != 1 {
		t.Fatalf("newest is %d on %d liteservers after 10 then 9", h.block.SeqNo, len(h.nodes))
	}
}
