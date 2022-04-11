package agg

type Datum[ID comparable, Attributes any] interface {
	ID() ID
	Attributes() Attributes
}

type Aggregator[Attributes any] func(Attributes) float64
