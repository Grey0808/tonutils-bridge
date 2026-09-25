package wsbridge

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// headAPI is a liteserver whose chain grows a block each time it is asked to
// wait for the next one, and whose cached head never moves.
type headAPI struct {
	ton.APIClientWrapped
	tip     atomic.Uint32
	waitFor atomic.Uint32
}

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

func TestAStateIsReadAtTheNewestBlockFollowedNotTheSecondOldHead(t *testing.T) {
	api := &headAPI{}
	api.tip.Store(100)
	b := &WSBridge{api: api}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.followHead(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		blk, _, err := b.stateHead(ctx)
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

func TestWithNoFollowedHeadAStateIsReadAtTheCachedOne(t *testing.T) {
	b := &WSBridge{api: &headAPI{}}
	blk, _, err := b.stateHead(context.Background())
	if err != nil || blk.SeqNo != 100 {
		t.Fatalf("%v %v", blk, err)
	}
	b.newest.Store(&followedHead{block: &ton.BlockIDExt{SeqNo: 200}, at: time.Now().Add(-time.Minute)})
	if blk, _, _ := b.stateHead(context.Background()); blk.SeqNo != 100 {
		t.Fatalf("a followed head a minute old was trusted: %d", blk.SeqNo)
	}
}

func TestAnOlderBlockNeverReplacesTheNewestNoted(t *testing.T) {
	b := &WSBridge{}
	b.noteHead(&ton.BlockIDExt{SeqNo: 10})
	b.noteHead(&ton.BlockIDExt{SeqNo: 9})
	if got := b.newest.Load().block.SeqNo; got != 10 {
		t.Fatalf("newest is %d after 10 then 9", got)
	}
}
