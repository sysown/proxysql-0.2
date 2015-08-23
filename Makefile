O0=-O0
O2=-O2
O1=-O1
O3=-O3 -mtune=native
#OPTZ=$(O2)
EXTRALINK=#-pg
ALL_DEBUG=-ggdb -DDEBUG
NO_DEBUG=
DEBUG=${ALL_DEBUG}
#export DEBUG
#export OPTZ
export EXTRALINK

all:
	OPTZ="${O2}" gmake default

.PHONY: default
default:
	gmake deps
	gmake lib
	gmake src

.PHONY: debug
debug:
	OPTZ="${O0}" DEBUG="${ALL_DEBUG}" gmake default

.PHONY: deps
deps:
	cd deps && gmake

.PHONY: lib
lib:
	cd lib && gmake -j 5

.PHONY: src
src:
	cd src && gmake

.PHONY: clean
clean:
	cd lib && gmake clean
	cd src && gmake clean

.PHONY: cleanall
cleanall:
	cd deps && gmake cleanall
	cd lib && gmake clean
	cd src && gmake clean
