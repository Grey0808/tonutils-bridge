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
// block, and state reads are made at the newest block named.
//
// It is followed on every liteserver the pool holds at once, not on the one
// the balancer picks, and a read goes to a liteserver that has already named
// the block it is made at. Public liteservers do not learn a block together:
// on 2026-09-26 eleven learned each within a few tens of milliseconds of one
// another and a twelfth 1.2 s after them, missing half outright. A single
// follower that landed on it held the head back, and a read the balancer
// sent to it waited there, server-side, for a block the rest already had.

// headFresh is how long a followed head is trusted with nothing newer heard.
// A masterchain block comes every few seconds; followers that have all gone
// quiet for longer than this have stopped, and the cached head is the
// fallback.
const headFresh = 10 * time.Second

// headNodesEvery is how often the pool is looked at for liteservers not yet
// followed; one it connects to later is followed from then on.
const headNodesEvery = time.Minute

type followedHead struct {
	block *ton.BlockIDExt
	at    time.Time
	// nodes are the liteservers that have named block, in the order they did.
	nodes []headNode
}

// headNode is a liteserver the head is followed on: its id in the pool, and
// a context pinned to it (see pinned).
type headNode struct {
	id  uint32
	pin context.Context
}

// followHead keeps b.newest at the masterchain's newest block until ctx ends.
func (b *WSBridge) followHead(ctx context.Context) {
	followed := map[uint32]bool{}
	for {
		lc := b.api.Client()
		// The pins carry nothing but the pin, so a context made from one
		// keeps its own deadline and cancellation.
		for _, pin := range liteNodes(context.Background(), lc) {
			n := headNode{id: lc.StickyNodeID(pin), pin: pin}
			if !followed[n.id] {
				followed[n.id] = true
				go b.followOn(ctx, n)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(headNodesEvery):
		}
	}
}

// followOn follows the head on liteserver n until ctx ends.
func (b *WSBridge) followOn(ctx context.Context, n headNode) {
	on := pinned{ctx, n.pin}
	var last uint32
	for ctx.Err() == nil {
		var blk *ton.BlockIDExt
		var err error
		if last == 0 {
			blk, err = b.api.GetMasterchainInfo(on)
		} else {
			blk, err = b.api.WaitForBlock(last + 1).GetMasterchainInfo(on)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Debug().Err(err).Uint32("node", n.id).Msg("following the masterchain head")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		b.noteHead(blk, n)
		last = blk.SeqNo
	}
}

// noteHead records that liteserver n has named blk: as the newest block if it
// is newer than the one recorded, and as one more liteserver that has it if it
// is the same.
func (b *WSBridge) noteHead(blk *ton.BlockIDExt, n headNode) {
	if blk == nil {
		return
	}
	for {
		cur := b.newest.Load()
		var next *followedHead
		switch {
		case cur == nil || blk.SeqNo > cur.block.SeqNo:
			next = &followedHead{block: blk, at: time.Now(), nodes: []headNode{n}}
		case blk.SeqNo < cur.block.SeqNo:
			return
		default:
			for _, had := range cur.nodes {
				if had.id == n.id {
					return
				}
			}
			nodes := append(append(make([]headNode, 0, len(cur.nodes)+1), cur.nodes...), n)
			next = &followedHead{block: cur.block, at: cur.at, nodes: nodes}
		}
		if b.newest.CompareAndSwap(cur, next) {
			return
		}
	}
}

// stateHead is the block to read an account's state at, the API to read it
// through and the context to read it in: the newest block followed, read on
// the liteservers that have named it, in turn — still told to wait for the
// block, since tonutils-go sends a query elsewhere when its liteserver cannot
// be reached — or, with no fresh followed head, tonutils-go's cached one on
// whichever liteserver the balancer picks.
func (b *WSBridge) stateHead(ctx context.Context) (context.Context, *ton.BlockIDExt, ton.APIClientWrapped, error) {
	if h := b.newest.Load(); h != nil && time.Since(h.at) < headFresh {
		if len(h.nodes) > 0 {
			ctx = pinned{ctx, h.nodes[b.headTurn.Add(1)%uint64(len(h.nodes))].pin}
		}
		return ctx, h.block, b.api.WaitForBlock(h.block.SeqNo), nil
	}
	blk, err := b.api.CurrentMasterchainInfo(ctx)
	return ctx, blk, b.api, err
}

// pinned is a context with the deadline, cancellation and values of its own
// and the liteserver pin of another. tonutils-go keeps a pin as a context
// value under a key of its own, and the only way to pin to a liteserver by
// its id, ConnectionPool.StickyContextWithNodeID, is out of reach behind the
// retrying client the bridge is given.
type pinned struct {
	context.Context
	pin context.Context
}

func (p pinned) Value(key any) any {
	if v := p.pin.Value(key); v != nil {
		return v
	}
	return p.Context.Value(key)
}

// newestHead is the field followHead keeps; see stateHead.
type newestHead = atomic.Pointer[followedHead]
