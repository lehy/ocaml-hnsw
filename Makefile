test:
	jbuilder runtest --force
	for i in _build/default/test/*.dot; do neato -Tpng -o "$$i".png "$$i"; done
	display _build/default/test/*.png

.PHONY: test
