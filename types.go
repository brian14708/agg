package agg

import (
	"golang.org/x/exp/constraints"
)

type number interface {
	constraints.Integer | constraints.Float | constraints.Complex
}

type DatumT[ID comparable, Score number] interface {
	ID() ID
	Aggregate() (min, max Score, complete bool)
}

type Fetcher[Datum DatumT[ID, Score], ID comparable, Score number] interface {
	ScanFields() []Iterator[Datum, ID, Score]
	GetDatum(Datum) (Datum, error)
	Merge(a, b Datum) Datum
}

type Iterator[Datum DatumT[ID, Score], ID comparable, Score number] interface {
	Next([]Datum) (int, error)
}
