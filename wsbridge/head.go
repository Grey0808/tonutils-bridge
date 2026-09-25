package wsbridge

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/ton"
)

// A read of an account's state is made at a masterchain block. tonutils-go's
// CurrentMasterchainInfo asks for the head at most once a second, so for up
// to a second after a block every read answers from the one before it — while
// a client following subscribe.newTransactions has already been told of the
// block, and prices on the state before it. The head is followed here instead:
// one long-poll at a time, answered the moment the liteserver has the next
// block, and state reads are made at the newest block it has named.

// headFresh is how long a followed head is trusted with nothing newer heard.
// A masterchain block comes every few seconds; a follower that has gone
// quiet for longer than this has stopped, and the cached head is the fallback.
const headFresh = 10 * time.Second

type followedHead struct {
	block *ton.BlockIDExt
	at    time.Time
}

// followHead keeps b.newest at the masterchain's newest block until ctx ends.
func (b *WSBridge) followHead(ctx context.Context) {
	var last uint32
	for ctx.Err() == nil {
		var blk *ton.BlockIDExt
		var err error
		if last == 0 {
			blk, err = b.api.GetMasterchainInfo(ctx)
		} else {
			blk, err = b.api.WaitForBlock(last + 1).GetMasterchainInfo(ctx)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Debug().Err(err).Msg("following the masterchain head")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		b.noteHead(blk)
		last = blk.SeqNo
	}
}

// noteHead records blk as the newest masterchain block if it is newer than
// the one recorded.
func (b *WSBridge) noteHead(blk *ton.BlockIDExt) {
	if blk == nil {
		return
	}
	next := &followedHead{block: blk, at: time.Now()}
	for {
		cur := b.newest.Load()
		if cur != nil && cur.block.SeqNo >= blk.SeqNo {
			return
		}
		if b.newest.CompareAndSwap(cur, next) {
			return
		}
	}
}

// stateHead is the block to read an account's state at, and the API to read
// it through: the newest block followed, read through a liteserver told to
// wait for it — the one a query reaches may be a moment behind the one that
// named it — or, with no fresh followed head, tonutils-go's cached one.
func (b *WSBridge) stateHead(ctx context.Context) (*ton.BlockIDExt, ton.APIClientWrapped, error) {
	if h := b.newest.Load(); h != nil && time.Since(h.at) < headFresh {
		return h.block, b.api.WaitForBlock(h.block.SeqNo), nil
	}
	blk, err := b.api.CurrentMasterchainInfo(ctx)
	return blk, b.api, err
}

// newestHead is the field followHead keeps; see stateHead.
type newestHead = atomic.Pointer[followedHead]
