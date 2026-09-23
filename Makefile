JAVAC   ?= javac
ROCKSDB_JNI_VER ?= 10.2.1
ROCKSDB_JAR := lib/rocksdbjni-$(ROCKSDB_JNI_VER).jar
JFLAGS  ?= -d classes -encoding UTF-8 --release 20 -cp classes:$(ROCKSDB_JAR)
JRUN    ?= java -cp classes:$(ROCKSDB_JAR)
SRCS    := $(wildcard src/shrt/*.java)
TESTS   := $(wildcard tests/*.java)

all: classes $(ROCKSDB_JAR)
	$(JAVAC) $(JFLAGS) $(SRCS)

$(ROCKSDB_JAR):
	mkdir -p lib
	curl -fsSL -o $@ https://repo1.maven.org/maven2/org/rocksdb/rocksdbjni/$(ROCKSDB_JNI_VER)/rocksdbjni-$(ROCKSDB_JNI_VER).jar

classes:
	mkdir -p classes

test-classes: classes $(ROCKSDB_JAR)
	$(JAVAC) $(JFLAGS) $(SRCS) $(TESTS)

test: test-classes
	$(JRUN) shrt.TestMain

bench: all
	$(JRUN) shrt.Bench

clean:
	rm -rf classes

.PHONY: all test bench clean
