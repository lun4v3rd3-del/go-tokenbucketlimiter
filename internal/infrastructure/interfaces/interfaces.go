package interfaces

import (
	"awesomeProject12/internal/usecase"
	"context"
)

type Limiter interface {
	Process(interface{}, interface{}, context.Context) bool
	Register(string, []usecase.FuncConfigPair, context.Context) error
	Getshards() []*usecase.Shard
}
