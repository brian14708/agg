package agg

type options struct {
	batchSize int
}

type Option func(*options)

func WithBatchSize(k int) Option {
	return func(opt *options) {
		opt.batchSize = k
	}
}
