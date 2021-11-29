package agg

import (
	"io"
	"sort"
)

type nra struct {
	opt        options
	a          *aggregator
	pool       map[interface{}]*entry
	poolIgnore map[interface{}]uint8
	q          *entryQ
	mergeFn    func(*Datum, Datum)
}

func (n *nra) nextRound(iter int, batchSize int) error {
	if batchSize < n.opt.batchSize {
		batchSize = n.opt.batchSize
	}
	results, err := n.a.fetch(batchSize)
	if err != nil {
		return err
	}

	for _, ret := range results {
		var r Datum
		for _, r = range ret {
			id := r.ID()

			if e, ok := n.poolIgnore[id]; ok {
				if e > 1 {
					n.poolIgnore[id]--
				} else {
					delete(n.poolIgnore, id)
				}
				continue
			}

			curr, ok := n.pool[id]
			if !ok {
				curr = &entry{
					unseenIterator: n.a.lenIt(),
				}
				n.pool[id] = curr
			}
			n.mergeFn(&curr.Datum, r)
			curr.iter = iter
			curr.unseenIterator--
		}
	}
	return nil
}

func (n *nra) fetchAtLeastK(k int) error {
	for len(n.pool) <= k {
		err := n.nextRound(0, k-len(n.pool))
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *nra) updatePool(iter int) {
	for k, v := range n.pool {
		var (
			attrs  = v.Datum.Fields()
			ignore = (v.unseenIterator == 0)
			final  bool
		)

		v.best, final = n.a.bestScore(attrs)

		if final {
			v.worst = v.best
			ignore = true
		} else if v.iter == iter {
			v.worst = n.a.worstScore(attrs)
			if v.worst == v.best {
				ignore = true
			}
		}
		if n.q.update(v) {
			ignore = true
		}
		if ignore {
			delete(n.pool, k)
			if v.unseenIterator != 0 {
				n.poolIgnore[k] = v.unseenIterator
			}
		}
	}
}

func NRA(fetcher Fetcher, agg Aggregator, k int, opts ...Option) ([]Datum, error) {
	n := nra{
		opt:        makeOptions(opts),
		a:          newAggregator(fetcher.ScanFields(), agg),
		pool:       make(map[interface{}]*entry),
		poolIgnore: make(map[interface{}]uint8),
		q:          newEntryQ(k),
		mergeFn:    fetcher.Merge,
	}

	// get at least k objects
	if err := n.fetchAtLeastK(k); err == io.EOF {
		cand := make([]Datum, 0, len(n.pool))
		score := make([]float64, 0, len(n.pool))
		for _, o := range n.pool {
			cand = append(cand, o.Datum)
			score = append(score, n.a.worstScore(o.Fields()))
		}
		sort.Sort(&sortByScore{cand, score})
		return cand, nil
	} else if err != nil {
		return nil, err
	}

	for iter := 0; ; iter++ {
		n.updatePool(iter)

		bk, ok := n.q.bestNotInTopK()
		if !ok {
			bk = n.a.bestUnseenScore()
		}
		if bk <= n.q.kthWorst() {
			break
		}

		if err := n.nextRound(iter+1, 0); err != nil {
			return nil, err
		}
	}
	return n.q.topK(), nil
}
