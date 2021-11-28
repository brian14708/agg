package agg

import (
	"io"
	"sort"
)

func NRA(fetcher Fetcher, agg Aggregator, k int, opts ...Option) ([]Datum, error) {
	opt := options{
		batchSize: 1,
	}
	for _, o := range opts {
		o(&opt)
	}

	it := fetcher.ScanFields()
	n := len(it)
	buf := make([]float64, 4*n)
	currMinField := buf[:n]
	minField := buf[n : 2*n]
	misField := buf[2*n : 3*n]
	tmpField := buf[3*n : 4*n]
	for i, v := range it {
		minField[i], currMinField[i], misField[i] = v.ValueRange()
	}

	var (
		pool       = make(map[interface{}]*entry)
		poolIgnore = make(map[interface{}]int)
		qq         = newEntryQ(k)
	)

	fetch := func(it []Iterator, batchSize int) ([][]Datum, error) {
		hasData := false
		if batchSize < opt.batchSize {
			batchSize = opt.batchSize
		}
		var results [][]Datum
		for i, c := range it {
			if c == nil {
				continue
			}
			ret, err := c.Next(batchSize)
			if err == io.EOF {
				it[i] = nil
				currMinField[i] = minField[i]
				continue
			}
			if err != nil {
				return nil, err
			}
			hasData = true
			results = append(results, ret)
		}
		if !hasData {
			return nil, io.EOF
		}
		return results, nil
	}

	nextRound := func(iter int, batchSize int) error {
		if batchSize < opt.batchSize {
			batchSize = opt.batchSize
		}
		results, err := fetch(it, batchSize)
		if err != nil {
			return err
		}

		for i, ret := range results {
			var r Datum
			for _, r = range ret {
				id := r.ID()

				if e, ok := poolIgnore[id]; ok {
					if e+1 == n {
						delete(poolIgnore, id)
					} else {
						poolIgnore[id]++
					}
					continue
				}

				curr, ok := pool[id]
				if !ok {
					curr = new(entry)
					pool[id] = curr
				}
				fetcher.Merge(&curr.Datum, r)
				curr.iter = iter
				curr.source++
			}
			currMinField[i] = r.Fields()[i]
		}
		return nil
	}

	for len(pool) <= k {
		err := nextRound(0, k-len(pool))
		if err == io.EOF {
			cand := make([]Datum, 0, len(pool))
			score := make([]float64, 0, len(pool))
			for _, o := range pool {
				cand = append(cand, o.Datum)
				score = append(score, agg(o.Datum.Fields()))
			}
			sort.Sort(&sortByScore{cand, score})
			return cand, nil
		} else if err != nil {
			return nil, err
		}
	}

	for iter := 0; ; iter++ {
		if err := nextRound(iter, 0); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		for k, v := range pool {
			attrs := v.Datum.Fields()
			missing := false
			for i, v := range attrs {
				if v == misField[i] {
					tmpField[i] = currMinField[i]
					missing = true
				} else {
					tmpField[i] = v
				}
			}
			v.best = agg(tmpField)

			var ignore = false
			if !missing {
				v.worst = v.best
				ignore = true
			} else if v.iter == iter {
				for i, v := range attrs {
					if v == misField[i] {
						tmpField[i] = minField[i]
					} else {
						tmpField[i] = v
					}
				}
				v.worst = agg(tmpField)
				if v.worst == v.best {
					ignore = true
				}
			}
			if qq.update(v) {
				ignore = true
			}
			if ignore {
				delete(pool, k)
				if v.source != n {
					poolIgnore[k] = v.source
				}
			}
		}

		bk, ok := qq.bestNotInTopK()
		if !ok {
			bk = agg(currMinField)
		}
		if bk <= qq.kthWorst() {
			break
		}
	}
	return qq.topK(), nil
}
