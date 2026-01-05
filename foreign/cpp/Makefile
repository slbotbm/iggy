CXX ?= g++
CXXFLAGS ?= -std=c++17
MODE ?= debug

ADDR ?= 127.0.0.1:8090
STREAM ?= example-stream
TOPIC ?= example-topic
COUNT ?= 5

ifeq ($(MODE),release)
	OPT_FLAGS := -O2
else
	OPT_FLAGS := -O0 -g
endif

TARGET_DIR := target/$(MODE)
RUST_LIB := $(TARGET_DIR)/libiggy.a
CXXBRIDGE_DIR := target/cxxbridge
INCLUDE_DIR := $(CXXBRIDGE_DIR)

PRODUCER_SRC := examples/producer/producer.cpp
CONSUMER_SRC := examples/consumer/consumer.cpp
PRODUCER_BIN := examples/producer/producer
CONSUMER_BIN := examples/consumer/consumer

.PHONY: all rust examples producer consumer run-producer run-consumer clean

all: examples

rust:
	cargo build $(if $(filter $(MODE),release),--release,)

$(RUST_LIB): rust

$(PRODUCER_BIN): $(PRODUCER_SRC) $(RUST_LIB)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(INCLUDE_DIR) $< $(RUST_LIB) -lpthread -ldl -o $@

$(CONSUMER_BIN): $(CONSUMER_SRC) $(RUST_LIB)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(INCLUDE_DIR) $< $(RUST_LIB) -lpthread -ldl -o $@

examples: producer consumer

producer: $(PRODUCER_BIN)

consumer: $(CONSUMER_BIN)

clean:
	rm -f $(PRODUCER_BIN) $(CONSUMER_BIN)
