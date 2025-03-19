package main

import (
	"bufio"
	"context"
	"fmt"
	"iter"
	"log"
	"os"
	"strconv"

	sp "github.com/takanoriyanagitani/go-serial2parquet"
	. "github.com/takanoriyanagitani/go-serial2parquet/util"
	pw "github.com/takanoriyanagitani/go-serial2parquet/writer"
)

var getEnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var compression IO[string] = getEnvValByKey("ENV_COMPRESSION").Or(Of(""))

var lines iter.Seq[string] = func(
	yield func(string) bool,
) {
	var s *bufio.Scanner = bufio.NewScanner(os.Stdin)
	for s.Scan() {
		var line string = s.Text()
		if !yield(line) {
			return
		}
	}
}

var serials iter.Seq[int32] = func(
	yield func(int32) bool,
) {
	for line := range lines {
		parsed, e := strconv.Atoi(line)
		if nil != e {
			return
		}

		if !yield(int32(parsed & 0x7fffffff)) {
			return
		}
	}
}

var chunks sp.SerialChunks = func(
	yield func(sp.SerialChunk) bool,
) {
	var page [4096]int32
	var buf []int32 = page[:0]

	for serial := range serials {
		if len(page) <= len(buf) {
			if !yield(buf) {
				return
			}
			buf = buf[:0]
		}

		buf = append(buf, serial)
	}

	if 0 < len(buf) {
		yield(buf)
	}
}

var iwriter IO[pw.Writer] = Bind(
	compression,
	Lift(func(comp string) (pw.Writer, error) {
		return pw.
			WriteConfigDefault.
			AddCompressOption(comp).
			SerialsWriter(os.Stdout), nil
	}),
)

var stdin2ints2parquet2stdout IO[Void] = Bind(
	iwriter,
	func(w pw.Writer) IO[Void] { return w(chunks) },
)

func main() {
	_, e := stdin2ints2parquet2stdout(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
