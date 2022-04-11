package agg

var (
	_ DatumT[string, float32]           = (*myData)(nil)
	_ Fetcher[*myData, string, float32] = (*fetcher)(nil)
)

type myData struct {
	key string
	// a, b in [0, 10]
	a float32
	b float32
}

func (m *myData) ID() string { return m.key }

func (m *myData) Aggregate() (min, max float32, complete bool) {
	if m.b < 0 {
		return m.a, m.a + 10, false
	} else if m.a < 0 {
		return m.b, m.b + 10, false
	}
	return m.a + m.b, m.a + m.b, true
}

type fetcher struct{}

func (*fetcher) ScanFields() []Iterator[*myData, string, float32] {
	return nil
}

func (*fetcher) GetDatum(*myData) (*myData, error) { return nil, nil }

func (*fetcher) Merge(a *myData, b *myData) *myData { return nil }
