package agg

type Datum interface {
	ID() interface{}
	Fields() []float64
}

type Aggregator func([]float64) float64

type FieldInfo struct {
	MinValue      float64
	MaxValue      float64
	SentinelValue float64

	// negative cost indicate unsupported
	ScanCost     int64
	GetCost      int64
	ScanIsSorted bool
}

type Iterator interface {
	Next(hint int) ([]Datum, error)
	Close()
}

type Fetcher interface {
	// list all fields meta info
	Fields() []FieldInfo

	// scan iterator for specified field
	ScanField(int) Iterator

	// fill all unknown fields in datum
	GetDatum(*Datum) error

	// merge all fields in two datum, usually returned by
	// different iterators
	//
	// when `*dst` is nil, datum should be copied into dst
	Merge(dst *Datum, src Datum)
}
