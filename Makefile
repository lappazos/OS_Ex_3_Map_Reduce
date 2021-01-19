CC=g++
CXX=g++
RANLIB=ranlib

LIBSRC=MapReduceFramework.cpp MultiThreadJob.cpp
LIBOBJ=$(LIBSRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

LIBMAPREDUCEFRAMEWORK = libMapReduceFramework.a
TARGETS = $(LIBMAPREDUCEFRAMEWORK)

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=$(LIBSRC) Makefile README MultiThreadJob.h fileWordCounter_comparison.png

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(TARGETS) $(LIBMAPREDUCEFRAMEWORK) $(OBJ) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)
