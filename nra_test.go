package agg

import (
	"io"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

type fetcher struct {
	orig          []datum
	index         [][]datum
	fieldsScanned int
}

type iter struct {
	val     []datum
	idx     int
	cursor  int
	buf     []datum
	cache   []Datum
	scanned *int
}

func (it *iter) valueRange() (min, max, missing float64) {
	return it.val[len(it.val)-1].t[it.idx], it.val[0].t[it.idx], -1
}

func (it *iter) Close() {}

func (it *iter) Next(count int) ([]Datum, error) {
	for len(it.buf) < count {
		d := datum{
			id: -1,
			t:  make([]float64, len(it.val[0].t)),
		}
		for i := range d.t {
			d.t[i] = -1.0
		}
		it.buf = append(it.buf, d)
	}

	var (
		buf  = it.buf
		data = it.val
		from = it.cursor
		idx  = it.idx
	)
	if from >= len(data) {
		return nil, io.EOF
	}
	ret := it.cache[:0]
	for i := from; i < from+count; i++ {
		if i >= len(data) {
			break
		}
		(*it.scanned)++
		d := buf[i-from]
		d.id = data[i].id
		d.t[idx] = data[i].t[idx]
		ret = append(ret, &d)
	}
	it.cache = ret
	it.cursor += len(ret)
	return ret, nil
}

func newFetcher(src []datum) *fetcher {
	index := make([][]datum, len(src[0].t))
	for idx := range index {
		tmp := make([]datum, len(src))
		copy(tmp, src)
		sort.Slice(tmp, func(i, j int) bool {
			return tmp[i].t[idx] > tmp[j].t[idx]
		})
		index[idx] = tmp
	}
	return &fetcher{
		orig:  src,
		index: index,
	}
}

func (f *fetcher) Fields() []FieldInfo {
	r := make([]FieldInfo, 0, len(f.index))
	for i, v := range f.index {
		r = append(r, FieldInfo{
			MinValue:      v[len(v)-1].t[i],
			MaxValue:      v[0].t[i],
			SentinelValue: -1,

			ScanIsSorted: true,
		})
	}
	return r
}

func (f *fetcher) ScanField(i int) Iterator {
	return &iter{
		val:     f.index[i],
		idx:     i,
		scanned: &f.fieldsScanned,
	}
}

func (f *fetcher) GetDatum(d *Datum) error {
	*d = &f.orig[(*d).(*datum).id]
	return nil
}

func (f *fetcher) Merge(dst *Datum, src Datum) {
	if *dst == nil {
		*dst = src.(*datum).clone()
		return
	}

	ddst := (*dst).(*datum)
	for i, v := range src.(*datum).t {
		if v >= 0.0 {
			ddst.t[i] = v
		}
	}
}

func TestNRA(t *testing.T) {
	{
		data := newFetcher([]datum{
			{1, []float64{0.3, 0.3}},
			{2, []float64{0.3, 0.3}},
			{3, []float64{0.3, 0.3}},
			{4, []float64{0.3, 0.3}},
			{99, []float64{1, 0}},
		})
		ret, err := NRA(data, max, 1)
		require.NoError(t, err)
		require.Equal(t, int64(99), ret[0].ID())
		require.Equal(t, 2, data.fieldsScanned)
	}
	{
		data := newFetcher([]datum{
			{1, []float64{0.3, 0.3}},
			{2, []float64{0.3, 0.3}},
			{3, []float64{0.3, 0.3}},
			{4, []float64{0.3, 0.3}},
			{99, []float64{1, 0}},
		})
		ret, err := NRA(data, sum, 1)
		require.NoError(t, err)
		require.Equal(t, int64(99), ret[0].ID())
		require.Equal(t, 2*2, data.fieldsScanned)
	}
	{
		data := generate(1000, 3)
		for i := 100; i < 1200; i += 100 {
			gt := naiveTopK(data, sum, i)
			ret, err := NRA(newFetcher(data), sum, i)
			require.NoError(t, err)
			if i <= len(data) {
				require.Equal(t, i, len(ret))
			} else {
				require.Equal(t, len(data), len(ret))
			}
			mp := make(map[interface{}]struct{})
			for _, g := range gt {
				mp[g.ID()] = struct{}{}
			}
			for _, r := range ret {
				if _, ok := mp[r.ID()]; !ok {
					panic("missing")
				}
			}
		}
	}
}

func BenchmarkNRA(b *testing.B) {
	data := generate(1000, 3)
	f := newFetcher(data)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NRA(f, sum, 10)
	}
	b.ReportMetric(float64(f.fieldsScanned)/(float64(len(data)*len(data[0].t)*b.N)), "scanned")
}
