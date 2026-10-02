package wsbridge

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// answeringNodes is a pool of liteservers numbered 1 to n, each answering with
// the liteserver error answers names for it, or with nothing wrong.
type answeringNodes struct {
	numberedNodes
	answers map[uint32]ton.LSError
	asked   []uint32
}

func (a *answeringNodes) QueryLiteserver(ctx context.Context, _ tl.Serializable, result tl.Serializable) error {
	id := a.StickyNodeID(ctx)
	a.asked = append(a.asked, id)
	if lsErr, ok := a.answers[id]; ok {
		*result.(*tl.Serializable) = lsErr
		return nil
	}
	*result.(*tl.Serializable) = ton.MasterchainInfo{}
	return nil
}

func lagging(shard, master int) ton.LSError {
	return ton.LSError{Code: 651, Text: "cannot load block (0,8000000000000000,100846879):AA:BB : block (0,8000000000000000,100846879) is not in db (possibly out of sync: shard_client_seqno=" +
		itoa(shard) + " ls_seqno=" + itoa(master) + ")"}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

func askOn(t *testing.T, w *SyncWatch, id uint32) {
	t.Helper()
	var resp tl.Serializable
	ctx := context.WithValue(context.Background(), pinKey{}, id)
	if err := w.QueryLiteserver(ctx, ton.GetMasterchainInf{}, &resp); err != nil {
		t.Fatal(err)
	}
}

func headOn(b *WSBridge, seqno uint32, ids ...uint32) {
	for _, id := range ids {
		b.noteHead(&ton.BlockIDExt{SeqNo: seqno}, headNode{id: id, pin: context.WithValue(context.Background(), pinKey{}, id)})
	}
}

func readsOn(b *WSBridge, n int) map[uint32]int {
	seen := map[uint32]int{}
	for i := 0; i < n; i++ {
		ctx, _, _, _ := b.stateHead(context.Background())
		seen[(numberedNodes{}).StickyNodeID(ctx)]++
	}
	return seen
}

func TestALiteserverWhoseShardClientLagsIsPassedOverByStateReadsForAWhile(t *testing.T) {
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{2: lagging(96354980, 96381184)}}
	w := WatchSync(nodes)
	now := time.Now()
	w.now = func() time.Time { return now }
	b := &WSBridge{api: &headAPI{}, syncs: w}
	headOn(b, 101, 1, 2, 3)

	askOn(t, w, 2)
	if seen := readsOn(b, 12); seen[2] != 0 || seen[1] != 6 || seen[3] != 6 {
		t.Fatalf("reads went to %v while liteserver 2's shard client lagged", seen)
	}
	now = now.Add(lagHold + time.Second)
	if seen := readsOn(b, 12); seen[2] != 4 {
		t.Fatalf("reads went to %v once the hold was over", seen)
	}
}

func TestALiteserverMerelyABlockBehindOnTheMasterchainIsNotPassedOver(t *testing.T) {
	for name, answer := range map[string]ton.LSError{
		"in step":   lagging(96447398, 96447398),
		"not found": {Code: 651, Text: "cannot load block wc=0 shard=8000000000000000 seqno=100961221: not found"},
		"other":     {Code: 652, Text: "shard_client_seqno=1 ls_seqno=9"},
	} {
		nodes := &answeringNodes{numberedNodes: numberedNodes{n: 2}, answers: map[uint32]ton.LSError{1: answer}}
		w := WatchSync(nodes)
		askOn(t, w, 1)
		if w.passedOver(1) {
			t.Fatalf("%s: %q holds the liteserver that gave it", name, answer.Text)
		}
	}
}

func TestWhenEveryLiteserverThatNamedTheHeadLagsAReadStillGoesToOneOfThem(t *testing.T) {
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{2: lagging(10, 50), 3: lagging(10, 50)}}
	w := WatchSync(nodes)
	b := &WSBridge{api: &headAPI{}, syncs: w}
	headOn(b, 101, 2, 3)
	askOn(t, w, 2)
	askOn(t, w, 3)
	if seen := readsOn(b, 10); seen[2] != 5 || seen[3] != 5 {
		t.Fatalf("reads went to %v, not to 2 and 3 in turn", seen)
	}
}

// The watch sits under tonutils-go's retries, so the liteserver a retry moved
// on from is held too, not only the last one asked.
func TestALiteserverARetryMovedOnFromIsHeld(t *testing.T) {
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{1: lagging(10, 50)}}
	w := WatchSync(nodes)
	api := ton.NewAPIClient(w).WithRetryTimeout(2, time.Second)
	var resp tl.Serializable
	ctx := context.WithValue(context.Background(), pinKey{}, uint32(1))
	if err := api.Client().QueryLiteserver(ctx, ton.GetMasterchainInf{}, &resp); err != nil {
		t.Fatal(err)
	}
	if _, ok := resp.(ton.MasterchainInfo); !ok || len(nodes.asked) != 2 || nodes.asked[1] != 2 {
		t.Fatalf("asked %v, answered %T", nodes.asked, resp)
	}
	if !w.passedOver(1) || w.passedOver(2) {
		t.Fatalf("held: 1 %v, 2 %v", w.passedOver(1), w.passedOver(2))
	}
}

func TestABlocksContentsAreReadOnALiteserverThatNamedItTellingItToWait(t *testing.T) {
	api := &headAPI{}
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{2: lagging(10, 50)}}
	w := WatchSync(nodes)
	b := &WSBridge{api: api, syncs: w}
	headOn(b, 101, 2, 3)
	askOn(t, w, 2)

	for i := 0; i < 4; i++ {
		ctx, _ := b.blockContents(context.Background(), 100)
		if got := (numberedNodes{}).StickyNodeID(ctx); got != 3 {
			t.Fatalf("block 100's contents read on liteserver %d", got)
		}
		if api.waitFor.Load() != 100 {
			t.Fatalf("the liteserver was told to wait for %d", api.waitFor.Load())
		}
	}
	// A block newer than any named is read wherever the balancer sends it,
	// still waited for.
	ctx, _ := b.blockContents(context.Background(), 102)
	if got := (numberedNodes{}).StickyNodeID(ctx); got != 0 || api.waitFor.Load() != 102 {
		t.Fatalf("block 102 read on %d, waited for %d", got, api.waitFor.Load())
	}
}

// A retry moves on past the liteservers already held rather than meeting one
// of them blind.
func TestARetryPassesOverTheLiteserversHeld(t *testing.T) {
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{1: lagging(10, 50), 2: lagging(10, 50)}}
	w := WatchSync(nodes)
	askOn(t, w, 2)
	nodes.asked = nil

	api := ton.NewAPIClient(w).WithRetryTimeout(2, time.Second)
	var resp tl.Serializable
	ctx := context.WithValue(context.Background(), pinKey{}, uint32(1))
	if err := api.Client().QueryLiteserver(ctx, ton.GetMasterchainInf{}, &resp); err != nil {
		t.Fatal(err)
	}
	if _, ok := resp.(ton.MasterchainInfo); !ok || len(nodes.asked) != 2 || nodes.asked[1] != 3 {
		t.Fatalf("asked %v, answered %T", nodes.asked, resp)
	}
}

func TestWhenEveryLiteserverLeftIsHeldARetryStillGoesToOneOfThem(t *testing.T) {
	nodes := &answeringNodes{numberedNodes: numberedNodes{n: 3}, answers: map[uint32]ton.LSError{1: lagging(10, 50), 2: lagging(10, 50), 3: lagging(10, 50)}}
	w := WatchSync(nodes)
	askOn(t, w, 2)
	askOn(t, w, 3)
	nodes.asked = nil

	api := ton.NewAPIClient(w).WithRetryTimeout(2, time.Second)
	var resp tl.Serializable
	ctx := context.WithValue(context.Background(), pinKey{}, uint32(1))
	_ = api.Client().QueryLiteserver(ctx, ton.GetMasterchainInf{}, &resp)
	if len(nodes.asked) != 3 || nodes.asked[1] != 2 || nodes.asked[2] != 3 {
		t.Fatalf("asked %v", nodes.asked)
	}
}

func TestAFailedBlockReadIsTriedAgainAtOnceThreeTimesThenASecondLater(t *testing.T) {
	for failures, want := range map[int]time.Duration{1: 100 * time.Millisecond, 3: 100 * time.Millisecond, 4: time.Second, 40: time.Second} {
		if got := contentsRetry(failures); got != want {
			t.Fatalf("after %d failures in a row the read waits %v, want %v", failures, got, want)
		}
	}
}
