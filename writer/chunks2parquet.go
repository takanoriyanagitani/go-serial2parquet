package wtr

import (
	"context"
	"errors"
	"io"
	"os"

	pq "github.com/parquet-go/parquet-go"
	pc "github.com/parquet-go/parquet-go/compress"
	pg "github.com/parquet-go/parquet-go/compress/gzip"
	pl "github.com/parquet-go/parquet-go/compress/lz4"
	ps "github.com/parquet-go/parquet-go/compress/snappy"
	pz "github.com/parquet-go/parquet-go/compress/zstd"
	sp "github.com/takanoriyanagitani/go-serial2parquet"
	. "github.com/takanoriyanagitani/go-serial2parquet/util"
)

var (
	ErrInvalidColumnBuffer error = errors.New("invalid column buffer")
	ErrInvalidSerialWriter error = errors.New("invalid serial writer")
)

type Writer func(sp.SerialChunks) IO[Void]

type SerialRow struct{ ID int32 }

func WriteSerialChunk(
	buf *pq.GenericBuffer[SerialRow],
	chunk sp.SerialChunk,
) error {
	var cols []pq.ColumnBuffer = buf.ColumnBuffers()
	if 1 != len(cols) {
		return ErrInvalidColumnBuffer
	}

	var col pq.ColumnBuffer = cols[0]
	wi32, ok := col.(pq.Int32Writer)
	if !ok {
		return ErrInvalidSerialWriter
	}

	_, e := wi32.WriteInt32s(chunk)
	return e
}

type SerialWriter struct {
	*pq.GenericWriter[SerialRow]
}

func (w SerialWriter) WriteRowGroup(rg pq.RowGroup) IO[int64] {
	return func(_ context.Context) (int64, error) {
		return w.GenericWriter.WriteRowGroup(rg)
	}
}

func (w SerialWriter) WriteBuf(buf *pq.GenericBuffer[SerialRow]) IO[int64] {
	return w.WriteRowGroup(buf)
}

type WriteConfig struct {
	Options    []pq.WriterOption
	BufOptions []pq.RowGroupOption
}

func (c WriteConfig) WithWriterOptions(opts ...pq.WriterOption) WriteConfig {
	c.Options = opts
	return c
}

func (c WriteConfig) AddWriteOptions(opts ...pq.WriterOption) WriteConfig {
	c.Options = append(c.Options, opts...)
	return c
}

var WriteConfigDefault WriteConfig = WriteConfig{
	Options:    nil,
	BufOptions: nil,
}

func (c WriteConfig) SerialsWriter(
	wtr io.Writer,
) Writer {
	return func(chunks sp.SerialChunks) IO[Void] {
		return func(ctx context.Context) (Void, error) {
			var gw *pq.GenericWriter[SerialRow] = pq.
				NewGenericWriter[SerialRow](
				wtr,
				c.Options...,
			)
			defer gw.Close()

			sw := SerialWriter{GenericWriter: gw}
			var buf *pq.GenericBuffer[SerialRow] = pq.
				NewGenericBuffer[SerialRow](
				c.BufOptions...,
			)

			for chunk := range chunks {
				select {
				case <-ctx.Done():
					return Empty, ctx.Err()
				default:
				}

				buf.Reset()
				e := WriteSerialChunk(buf, chunk)
				if nil != e {
					return Empty, e
				}

				_, e = sw.WriteBuf(buf)(ctx)
				if nil != e {
					return Empty, e
				}
			}

			return Empty, gw.Flush()
		}
	}
}

var WriteToStdoutDefault Writer = WriteConfigDefault.SerialsWriter(os.Stdout)

func CompressOptionGzipFast() pq.WriterOption {
	var codec pc.Codec = &pg.Codec{Level: pg.BestSpeed}
	return pq.Compression(codec)
}

func CompressOptionGzipBest() pq.WriterOption {
	var codec pc.Codec = &pg.Codec{Level: pg.BestCompression}
	return pq.Compression(codec)
}

func CompressOptionLz4Fast() pq.WriterOption {
	var codec pc.Codec = &pl.Codec{Level: pl.Fast}
	return pq.Compression(codec)
}

func CompressOptionSnappy() pq.WriterOption {
	var codec pc.Codec = &ps.Codec{}
	return pq.Compression(codec)
}

func CompressOptionZstdBest() pq.WriterOption {
	var codec pc.Codec = &pz.Codec{
		Level:       pz.SpeedBestCompression,
		Concurrency: pz.DefaultConcurrency,
	}
	return pq.Compression(codec)
}

func CompressOptionZstdFast() pq.WriterOption {
	var codec pc.Codec = &pz.Codec{
		Level:       pz.SpeedFastest,
		Concurrency: pz.DefaultConcurrency,
	}
	return pq.Compression(codec)
}

type CompressOptionMap map[string]func() pq.WriterOption

var CompOptMap CompressOptionMap = map[string]func() pq.WriterOption{
	"gzip-fast": CompressOptionGzipFast,
	"gzip-best": CompressOptionGzipBest,
	"lz4-fast":  CompressOptionLz4Fast,
	"snappy":    CompressOptionSnappy,
	"zstd-fast": CompressOptionZstdFast,
	"zstd-best": CompressOptionZstdBest,
}

func (c WriteConfig) AddCompressOption(ctyp string) WriteConfig {
	val, found := CompOptMap[ctyp]
	if !found {
		return c
	}

	var opt pq.WriterOption = val()
	return c.AddWriteOptions(opt)
}
