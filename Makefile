JAVAC   ?= javac
JFLAGS  ?= -d classes -encoding UTF-8 --release 20
SRCS    := $(wildcard src/shrt/*.java)
TESTS   := $(wildcard tests/*.java)

all: classes
	$(JAVAC) $(JFLAGS) $(SRCS)

classes:
	mkdir -p classes

test-classes: classes
	$(JAVAC) $(JFLAGS) -cp classes $(SRCS) $(TESTS)

test: test-classes
	java -cp classes shrt.TestMain

bench: all
	java -cp classes shrt.Bench

clean:
	rm -rf classes

.PHONY: all test bench clean
