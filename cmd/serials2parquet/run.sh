#!/bin/sh

input=./sample.d/input.ints.txt
output=./sample.d/out.parquet

count=131072

geninput(){
	echo generating input...

	mkdir -p sample.d

	seq 1 ${count} |
		dd \
			if=/dev/stdin \
			of="${input}" \
			bs=1048576 \
			status=progress
}

test -f "${input}" || geninput

export ENV_COMPRESSION=gzip-fast
export ENV_COMPRESSION=gzip-best
export ENV_COMPRESSION=lz4-fast
export ENV_COMPRESSION=snappy
export ENV_COMPRESSION=zstd-fast
export ENV_COMPRESSION=zstd-best

export ENV_COMPRESSION=zstd-best

cat "${input}" |
	./serials2parquet |
	dd \
		if=/dev/stdin \
		of="${output}" \
		bs=1048576 \
		conv=fsync \
		status=progress

file "${output}"

which duckdb | fgrep -q duckdb || exec sh -c 'echo duckdb missing.; exit 0'

duckdb \
	-c "
		SELECT COUNT(*) FROM './sample.d/out.parquet'
	"

ls -lS ./sample.d/*
