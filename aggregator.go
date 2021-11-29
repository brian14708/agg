package agg

import (
	"io"
	"math"
)

type aggregator struct {
	aggFn         Aggregator
	iterators     []Iterator
	currMinFields []float64
	minFields     []float64
	sentialFields []uint64

	buffer []float64
	ret    [][]Datum
}

func newAggregator(it []Iterator, agg Aggregator) *aggregator {
	n := len(it)
	buf := make([]float64, 3*n)
	currMinField := buf[:n]
	minField := buf[n : 2*n]
	tmpField := buf[2*n : 3*n]
	sentField := make([]uint64, n)
	for i, v := range it {
		var s float64
		minField[i], currMinField[i], s = v.ValueRange()
		sentField[i] = math.Float64bits(s)
	}
	return &aggregator{
		aggFn:         agg,
		iterators:     it,
		currMinFields: currMinField,
		minFields:     minField,
		sentialFields: sentField,
		buffer:        tmpField,
		ret:           make([][]Datum, 0, n),
	}
}

func (a *aggregator) fetch(sz int) ([][]Datum, error) {
	eof := 0
	results := a.ret[:0]
	for i, c := range a.iterators {
		if c == nil {
			eof += 1
			continue
		}
		ret, err := c.Next(sz)
		if err == io.EOF {
			a.iterators[i] = nil
			a.currMinFields[i] = a.minFields[i]
			continue
		}
		if err != nil {
			return nil, err
		}
		results = append(results, ret)

		if len(ret) > 0 {
			a.currMinFields[i] = ret[len(ret)-1].Fields()[i]
		}
	}
	if eof == len(a.iterators) {
		return nil, io.EOF
	}
	return results, nil
}

func (a *aggregator) worstScore(attrs []float64) float64 {
	tmpField := a.buffer
	for i, v := range attrs {
		if math.Float64bits(v) == a.sentialFields[i] {
			tmpField[i] = a.minFields[i]
		} else {
			tmpField[i] = v
		}
	}
	return a.aggFn(tmpField)
}

func (a *aggregator) bestScore(attrs []float64) (s float64, final bool) {
	tmpField := a.buffer
	missing := false
	for i, v := range attrs {
		if math.Float64bits(v) == a.sentialFields[i] {
			tmpField[i] = a.currMinFields[i]
			missing = true
		} else {
			tmpField[i] = v
		}
	}
	return a.aggFn(tmpField), !missing
}

func (a *aggregator) bestUnseenScore() float64 {
	return a.aggFn(a.currMinFields)
}

func (a *aggregator) lenIt() uint8 {
	l := len(a.iterators)
	if l >= 256 {
		panic("too many iterators")
	}
	return uint8(l)
}
