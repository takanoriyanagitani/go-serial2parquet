package ser2parquet

import (
	"context"
	"iter"
)

type Serial int32

type SerialChunk []int32

type SerialChunks iter.Seq[SerialChunk]

type WriteSerials func(context.Context, SerialChunks) error
