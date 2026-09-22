package wsbridge

import (
	"context"
	"fmt"
	"maps"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/ton"
)

// maxShardWalk bounds how far back one shard is read. A subscription reads
// every masterchain block in turn, and each names a shard at most a few
// blocks on from the one before it, so a longer walk means the bookkeeping is
// wrong — and reading hundreds of blocks to find that out would hold every
// event up behind it.
const maxShardWalk = 64

// shardBlocks is what the walk reads from a liteserver. apiShardBlocks adapts
// the bridge's API client to it; the tests use a fake.
type shardBlocks interface {
	LookupBlock(ctx context.Context, workchain int32, shard int64, seqno uint32) (*ton.BlockIDExt, error)
	Parents(ctx context.Context, block *ton.BlockIDExt) ([]*ton.BlockIDExt, error)
}

type apiShardBlocks struct{ api ton.APIClientWrapped }

func (a apiShardBlocks) LookupBlock(ctx context.Context, workchain int32, shard int64, seqno uint32) (*ton.BlockIDExt, error) {
	return a.api.LookupBlock(ctx, workchain, shard, seqno)
}

// Parents downloads the whole block for its header, so it is only asked for
// a shard the walk has not emitted from, which happens after a split or a
// merge.
func (a apiShardBlocks) Parents(ctx context.Context, block *ton.BlockIDExt) ([]*ton.BlockIDExt, error) {
	data, err := a.api.GetBlockData(ctx, block)
	if err != nil {
		return nil, err
	}
	return ton.GetParentBlocks(&data.BlockInfo)
}

type shardKey struct {
	workchain int32
	shard     int64
}

func shardKeyOf(b *ton.BlockIDExt) shardKey { return shardKey{b.Workchain, b.Shard} }

// shardWalk keeps, for one subscription, the newest block of each shard it
// has emitted.
//
// A masterchain block names each shard's newest block, and that is not every
// block: when two blocks of a shard are committed under one masterchain block,
// only the second is named, and a subscription that reads only what is named
// never emits the first. On mainnet that was one basechain block in seven. The
// reverse happens too — a shard with no new block is named again under the
// next masterchain block, and its transactions were sent twice.
type shardWalk struct {
	last map[shardKey]uint32
}

// start records the shards of the block the subscription starts after, which
// are not emitted.
func (w *shardWalk) start(tops []*ton.BlockIDExt) {
	w.last = make(map[shardKey]uint32, len(tops))
	for _, t := range tops {
		w.last[shardKeyOf(t)] = t.SeqNo
	}
}

// unseen returns every block up to the shard blocks tops names that has not
// been emitted yet — each shard's oldest first — and the bookkeeping to commit
// once they have been. Nothing is recorded here, so a masterchain block whose
// transactions fail to read is walked again from the same place.
//
// Only the shards named now are kept for next time: a shard that was split or
// merged away must not be walked back to later under the same id.
func (w *shardWalk) unseen(ctx context.Context, src shardBlocks, tops []*ton.BlockIDExt) ([]*ton.BlockIDExt, map[shardKey]uint32, error) {
	next := make(map[shardKey]uint32, len(tops))
	for _, t := range tops {
		next[shardKeyOf(t)] = t.SeqNo
	}
	if w.last == nil {
		// Nothing to walk back to: the start block's shards could not be read.
		return tops, next, nil
	}
	seen := maps.Clone(w.last)
	var out []*ton.BlockIDExt
	for _, top := range tops {
		blocks, err := walkBack(ctx, src, top, seen, 0)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, blocks...)
	}
	return out, next, nil
}

func (w *shardWalk) commit(next map[shardKey]uint32) { w.last = next }

// walkBack returns the blocks from the one after the last emitted of block's
// shard up to block itself, and marks them emitted in seen. Two shards split
// from one share a parent, and seen is what keeps it from being emitted twice.
func walkBack(ctx context.Context, src shardBlocks, block *ton.BlockIDExt, seen map[shardKey]uint32, depth int) ([]*ton.BlockIDExt, error) {
	k := shardKeyOf(block)
	if last, ok := seen[k]; ok {
		if block.SeqNo <= last {
			return nil, nil
		}
		from := last + 1
		if block.SeqNo-from > maxShardWalk {
			log.Warn().Int32("workchain", block.Workchain).Str("shard", fmt.Sprintf("%016x", uint64(block.Shard))).
				Uint32("last", last).Uint32("seqno", block.SeqNo).Msg("shard gap too long to walk, reading only its end")
			from = block.SeqNo - maxShardWalk
		}
		var out []*ton.BlockIDExt
		// The blocks between share the shard id, so each is found by seqno
		// alone, without downloading the block that names it.
		for seq := from; seq < block.SeqNo; seq++ {
			b, err := src.LookupBlock(ctx, block.Workchain, block.Shard, seq)
			if err != nil {
				return nil, fmt.Errorf("lookup block %d:%016x:%d: %w", block.Workchain, uint64(block.Shard), seq, err)
			}
			out = append(out, b)
		}
		seen[k] = block.SeqNo
		return append(out, block), nil
	}
	// A shard nothing has been emitted from: it was split or merged since the
	// last masterchain block, and its parents are named in its header.
	if depth >= maxShardWalk {
		log.Warn().Int32("workchain", block.Workchain).Str("shard", fmt.Sprintf("%016x", uint64(block.Shard))).
			Uint32("seqno", block.SeqNo).Msg("no emitted parent found for shard block, emitting it alone")
		seen[k] = block.SeqNo
		return []*ton.BlockIDExt{block}, nil
	}
	parents, err := src.Parents(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("parents of block %d:%016x:%d: %w", block.Workchain, uint64(block.Shard), block.SeqNo, err)
	}
	var out []*ton.BlockIDExt
	for _, p := range parents {
		blocks, err := walkBack(ctx, src, p, seen, depth+1)
		if err != nil {
			return nil, err
		}
		out = append(out, blocks...)
	}
	seen[k] = block.SeqNo
	return append(out, block), nil
}
