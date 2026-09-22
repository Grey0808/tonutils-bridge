package wsbridge

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

var (
	shardWhole = int64(-0x8000000000000000) // 0x8000000000000000
	shardLeft  = int64(0x4000000000000000)
	shardRight = int64(-0x4000000000000000) // 0xc000000000000000
)

func blk(shard int64, seqno uint32) *ton.BlockIDExt {
	return &ton.BlockIDExt{Workchain: 0, Shard: shard, SeqNo: seqno}
}

type fakeShardBlocks struct {
	parents map[string][]*ton.BlockIDExt
	lookups []string
	failAt  uint32
}

func blkName(shard int64, seqno uint32) string { return fmt.Sprintf("%016x:%d", uint64(shard), seqno) }

func (f *fakeShardBlocks) LookupBlock(_ context.Context, _ int32, shard int64, seqno uint32) (*ton.BlockIDExt, error) {
	if f.failAt != 0 && seqno == f.failAt {
		return nil, errors.New("timeout")
	}
	f.lookups = append(f.lookups, blkName(shard, seqno))
	return blk(shard, seqno), nil
}

func (f *fakeShardBlocks) Parents(_ context.Context, b *ton.BlockIDExt) ([]*ton.BlockIDExt, error) {
	p, ok := f.parents[blkName(b.Shard, b.SeqNo)]
	if !ok {
		return nil, fmt.Errorf("no parents for %s", blkName(b.Shard, b.SeqNo))
	}
	return p, nil
}

func names(bs []*ton.BlockIDExt) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = blkName(b.Shard, b.SeqNo)
	}
	return out
}

func sameNames(t *testing.T, got []*ton.BlockIDExt, want ...string) {
	t.Helper()
	g := names(got)
	if fmt.Sprint(g) != fmt.Sprint(want) {
		t.Fatalf("emitted %v, want %v", g, want)
	}
}

func TestShardWalk_ReadsBackBlocksCommittedUnderOneMasterchainBlock(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardWhole, 10)})
	src := &fakeShardBlocks{}
	got, next, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 13)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got, blkName(shardWhole, 11), blkName(shardWhole, 12), blkName(shardWhole, 13))
	if next[shardKey{0, shardWhole}] != 13 {
		t.Fatalf("next %v", next)
	}
}

func TestShardWalk_ShardBlockNamedAgainIsNotEmittedTwice(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardWhole, 10)})
	got, _, err := w.unseen(context.Background(), &fakeShardBlocks{}, []*ton.BlockIDExt{blk(shardWhole, 10)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got)
}

func TestShardWalk_NothingIsRecordedUntilCommitted(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardWhole, 10)})
	src := &fakeShardBlocks{failAt: 12}
	if _, _, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 13)}); err == nil {
		t.Fatal("a failed lookup was not reported")
	}
	src.failAt = 0
	got, next, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 13)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got, blkName(shardWhole, 11), blkName(shardWhole, 12), blkName(shardWhole, 13))
	w.commit(next)
	got, _, _ = w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 14)})
	sameNames(t, got, blkName(shardWhole, 14))
}

func TestShardWalk_WithoutAStartBlockEmitsWhatIsNamed(t *testing.T) {
	w := &shardWalk{}
	got, _, err := w.unseen(context.Background(), &fakeShardBlocks{}, []*ton.BlockIDExt{blk(shardWhole, 13)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got, blkName(shardWhole, 13))
}

func TestShardWalk_SplitEmitsTheSharedParentOnce(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardWhole, 9)})
	src := &fakeShardBlocks{parents: map[string][]*ton.BlockIDExt{
		blkName(shardLeft, 11):  {blk(shardWhole, 10)},
		blkName(shardRight, 11): {blk(shardWhole, 10)},
	}}
	got, next, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardLeft, 11), blk(shardRight, 11)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got, blkName(shardWhole, 10), blkName(shardLeft, 11), blkName(shardRight, 11))
	if _, ok := next[shardKey{0, shardWhole}]; ok || len(next) != 2 {
		t.Fatalf("the split shard is still kept: %v", next)
	}
}

func TestShardWalk_MergeReadsBothParentsUpToWhatWasEmitted(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardLeft, 19), blk(shardRight, 21)})
	src := &fakeShardBlocks{parents: map[string][]*ton.BlockIDExt{
		blkName(shardWhole, 22): {blk(shardLeft, 20), blk(shardRight, 21)},
	}}
	got, _, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 22)})
	if err != nil {
		t.Fatal(err)
	}
	sameNames(t, got, blkName(shardLeft, 20), blkName(shardWhole, 22))
}

func TestShardWalk_LongGapReadsOnlyItsEnd(t *testing.T) {
	w := &shardWalk{}
	w.start([]*ton.BlockIDExt{blk(shardWhole, 10)})
	src := &fakeShardBlocks{}
	got, _, err := w.unseen(context.Background(), src, []*ton.BlockIDExt{blk(shardWhole, 10+maxShardWalk+50)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != maxShardWalk+1 || len(src.lookups) != maxShardWalk {
		t.Fatalf("emitted %d blocks after %d lookups", len(got), len(src.lookups))
	}
}
