.VERBOSE:

VERSION ?= new

test:
	jbuilder runtest --force

test-2d:
	jbuilder exec test/test.exe --force
	for i in *.dot; do neato -Tpng -o "$$i".png "$$i"; done
	display *.png

perf-record:
	jbuilder build benchmark/benchmark.exe
	perf record -b --call-graph=dwarf -- _build/default/benchmark/benchmark.exe $(VERSION)

perf-report:
	perf report --hierarchy

.PHONY: test perf-record perf-report
