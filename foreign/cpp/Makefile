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
RUST_LIB := $(TARGET_DIR)/libiggy_cpp.a
CXXBRIDGE_DIR := target/cxxbridge
INCLUDE_DIR := $(CXXBRIDGE_DIR)
PUBLIC_INCLUDE_DIR := include
EXAMPLES_DIR := target/examples
CPP_SOURCES := src/iggy_client.cpp src/type_conversions.cpp

PRODUCER_SRC := examples/producer/producer.cpp
CONSUMER_SRC := examples/consumer/consumer.cpp
PRODUCER_BIN := $(EXAMPLES_DIR)/producer
CONSUMER_BIN := $(EXAMPLES_DIR)/consumer

.PHONY: all rust examples producer consumer run-producer run-consumer clean

all: examples

rust:
	cargo build $(if $(filter $(MODE),release),--release,)

$(RUST_LIB): rust

$(EXAMPLES_DIR):
	mkdir -p $@

$(PRODUCER_BIN): $(PRODUCER_SRC) $(CPP_SOURCES) $(RUST_LIB) | $(EXAMPLES_DIR)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(PUBLIC_INCLUDE_DIR) -I$(INCLUDE_DIR) $(PRODUCER_SRC) $(CPP_SOURCES) $(RUST_LIB) -lpthread -ldl -o $@

$(CONSUMER_BIN): $(CONSUMER_SRC) $(CPP_SOURCES) $(RUST_LIB) | $(EXAMPLES_DIR)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(PUBLIC_INCLUDE_DIR) -I$(INCLUDE_DIR) $(CONSUMER_SRC) $(CPP_SOURCES) $(RUST_LIB) -lpthread -ldl -o $@

examples: producer consumer

producer: $(PRODUCER_BIN)

consumer: $(CONSUMER_BIN)

clean:
	rm -f $(PRODUCER_BIN) $(CONSUMER_BIN)
