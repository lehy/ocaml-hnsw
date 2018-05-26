.VERBOSE:

VERSION ?= new

test:
	jbuilder runtest --force
	for i in _build/default/test/*.dot; do neato -Tpng -o "$$i".png "$$i"; done
	display _build/default/test/*.png

perf-record:
	jbuilder build benchmark/benchmark.exe
	perf record -b --call-graph=dwarf -- _build/default/benchmark/benchmark.exe $(VERSION)

perf-report:
	perf report --hierarchy

.PHONY: test perf-record perf-report
