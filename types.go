package agg

type Datum interface {
	ID() interface{}
	Fields() []float64
}

type Aggregator func([]float64) float64

type Iterator interface {
	Next(count int) ([]Datum, error)
	ValueRange() (min, max, sentinel float64)
}

type Fetcher interface {
	// descending order
	ScanFields() []Iterator
	GetDatum(*Datum) error

	Merge(dst *Datum, src Datum)
}
