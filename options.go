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

func makeOptions(opts []Option) options {
	opt := options{
		batchSize: 1,
	}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}
