package wsbridge

import (
	"context"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// A liteserver can name the newest masterchain block while its shard client,
// which applies the shard blocks under it, is still blocks behind. A state
// read made there at that block fails with code 651, "block is not in db
// (possibly out of sync: shard_client_seqno=X ls_seqno=Y)", and every read
// sent there fails the same way until the shard client catches up. On
// 2026-10-01 such liteservers were 27 to 454 masterchain blocks behind, one
// of them 26,204, and the arb engine halted at 21:09 on a wallet read that
// met three of them in a row. tonutils-go asks another liteserver after such
// an answer, but stateHead sent the next read back to the lagging one in its
// turn.
//
// SyncWatch sits between tonutils-go and the pool and notes every liteserver
// that answers so, retries included; stateHead passes over it for lagHold.
// A liteserver that is merely a block behind on the masterchain itself
// (X == Y) is not held: it has the block a moment later, and the reads that
// meet it wait for the block (waitMasterchainSeqno) rather than fail.

// lagHold is how long a liteserver whose shard client lagged is passed over.
// A shard client tens of blocks behind needs more than this to catch up, so
// a lagging liteserver costs one failed attempt, which tonutils-go retries
// elsewhere, every lagHold rather than one read in every few.
const lagHold = 30 * time.Second

// SyncWatch is a ton.LiteClient that notes the liteservers whose shard client
// lags (see above).
type SyncWatch struct {
	ton.LiteClient

	mu   sync.Mutex
	held map[uint32]time.Time // liteserver id → passed over until
	now  func() time.Time
}

// WatchSync wraps lc so that the liteservers whose shard client lags are
// noted as they answer.
func WatchSync(lc ton.LiteClient) *SyncWatch {
	return &SyncWatch{LiteClient: lc, held: map[uint32]time.Time{}, now: time.Now}
}

func (w *SyncWatch) QueryLiteserver(ctx context.Context, payload tl.Serializable, result tl.Serializable) error {
	err := w.LiteClient.QueryLiteserver(ctx, payload, result)
	if err != nil {
		return err
	}
	tmp, ok := result.(*tl.Serializable)
	if !ok || tmp == nil {
		return nil
	}
	lsErr, ok := (*tmp).(ton.LSError)
	if !ok || lsErr.Code != 651 {
		return nil
	}
	if behind, ok := shardClientBehind(lsErr.Text); ok && behind > 0 {
		if id := w.LiteClient.StickyNodeID(ctx); id != 0 {
			w.hold(id, behind)
		}
	}
	return nil
}

// StickyContextNextNodeBalanced is the liteserver tonutils-go's retries move
// on to, passing over the ones held: in eleven minutes of 2026-10-02, 13 of
// the 18 public liteservers answered that their shard client lagged, so a
// retry picked blind met another one about as often as not. When every
// liteserver left is held, it takes the first of them, as it would have.
func (w *SyncWatch) StickyContextNextNodeBalanced(ctx context.Context) (context.Context, error) {
	var first context.Context
	for {
		next, err := w.LiteClient.StickyContextNextNodeBalanced(ctx)
		if err != nil {
			if first != nil {
				return first, nil
			}
			return next, err
		}
		if !w.passedOver(w.LiteClient.StickyNodeID(next)) {
			return next, nil
		}
		if first == nil {
			first = next
		}
		ctx = next
	}
}

func (w *SyncWatch) hold(id uint32, behind int64) {
	now := w.now()
	w.mu.Lock()
	was, had := w.held[id]
	w.held[id] = now.Add(lagHold)
	w.mu.Unlock()
	if !had || !was.After(now) {
		log.Info().Uint32("node", id).Int64("behind", behind).
			Msg("a liteserver's shard client lags its masterchain; state reads pass over it for 30s")
	}
}

// passedOver says whether liteserver id answered that its shard client lags
// within the last lagHold.
func (w *SyncWatch) passedOver(id uint32) bool {
	if w == nil {
		return false
	}
	now := w.now()
	w.mu.Lock()
	defer w.mu.Unlock()
	until, ok := w.held[id]
	if ok && !until.After(now) {
		delete(w.held, id)
		return false
	}
	return ok
}

var outOfSync = regexp.MustCompile(`shard_client_seqno=(\d+) ls_seqno=(\d+)`)

// shardClientBehind reads how many masterchain blocks a 651 answer says the
// liteserver's shard client is behind its masterchain.
func shardClientBehind(text string) (int64, bool) {
	m := outOfSync.FindStringSubmatch(text)
	if m == nil {
		return 0, false
	}
	shard, err1 := strconv.ParseInt(m[1], 10, 64)
	master, err2 := strconv.ParseInt(m[2], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, false
	}
	return master - shard, true
}
