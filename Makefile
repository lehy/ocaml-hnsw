.VERBOSE:

VERSION ?= new

test:
	jbuilder runtest --dev --force

test-2d:
	jbuilder exec --dev test/test.exe --force
	for i in *.dot; do neato -Tpng -o "$$i".png "$$i"; done
	display *.png

perf-record:
	jbuilder build --dev benchmark/benchmark.exe
	perf record -b --call-graph=dwarf -- _build/default/benchmark/benchmark.exe $(VERSION)

bench:
	jbuilder build --dev benchmark/benchmark.exe
	_build/default/benchmark/benchmark.exe $(VERSION)

benchpy:
	python3 benchmark/bench.py

perf-report:
	perf report --hierarchy

download-data:
	cd data && wget 'http://vectors.erikbern.com/fashion-mnist-784-euclidean.hdf5'

.PHONY: test test-2d perf-record perf-report download-data bench benchpy
