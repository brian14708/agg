package agg

import (
	"math"
	"math/rand"
	"sort"
)

type datum struct {
	id int64
	t  []float64
}

func (d *datum) ID() interface{}   { return d.id }
func (d *datum) Fields() []float64 { return d.t }
func (d *datum) clone() *datum {
	t := make([]float64, len(d.t))
	copy(t, d.t)

	tmp := *d
	tmp.t = t
	return &tmp
}

func generate(n, m int) []datum {
	var ret []datum
	for i := 0; i < n; i++ {
		t := make([]float64, 0, m)
		for j := 0; j < m; j++ {
			t = append(t, rand.Float64())
		}
		ret = append(ret, datum{
			id: int64(i),
			t:  t,
		})
	}
	return ret
}

func sum(f []float64) float64 {
	ret := 0.0
	for _, ff := range f {
		ret += ff
	}
	return ret
}

func max(f []float64) float64 {
	ret := f[0]
	for _, ff := range f {
		ret = math.Max(ret, ff)
	}
	return ret
}

func naiveTopK(data []datum, agg Aggregator, k int) []datum {
	if k >= len(data) {
		k = len(data)
	}
	type elem struct {
		idx   int
		score float64
	}
	var tmp []elem
	for i, d := range data {
		tmp = append(tmp, elem{
			idx:   i,
			score: agg(d.t),
		})
	}
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].score > tmp[j].score
	})
	var ret []datum
	for _, t := range tmp[:k] {
		ret = append(ret, data[t.idx])
	}
	kth := tmp[k-1]
	for _, t := range tmp[k:] {
		if t.score == kth.score {
			ret = append(ret, data[t.idx])
		} else {
			break
		}
	}
	return ret
}
