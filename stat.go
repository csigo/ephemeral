package ephemeral

type dummyStat struct{}

type dummyEnd struct{}

func (d dummyStat) BumpAvg(key string, val float64) {}

func (d dummyStat) BumpSum(key string, val float64) {}

func (d dummyStat) BumpHistogram(key string, val float64) {}

func (d dummyStat) BumpTime(key string) interface {
	End()
} {
	return dummyEnd{}
}

func (d dummyEnd) End() {}
