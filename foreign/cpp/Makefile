# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

CXX ?= g++
MODE ?= debug
CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic
LDFLAGS ?= -lpthread -ldl

ifeq ($(MODE),release)
	OPT_FLAGS := -O2
	CARGO_FLAGS := --release
else
	OPT_FLAGS := -O0 -g
	CARGO_FLAGS :=
endif

TARGET_DIR := target/$(MODE)
RUST_LIB := $(TARGET_DIR)/libiggy_cpp.a
CXXBRIDGE_INCLUDE = $(shell ls -d $(TARGET_DIR)/build/iggy-cpp-*/out/cxxbridge/include 2>/dev/null | head -n 1)
PUBLIC_INCLUDE_DIR := include
EXAMPLES_DIR := target/examples
CPP_SOURCES := src/iggy_client.cpp

PRODUCER_SRC := examples/producer/producer.cpp
CONSUMER_SRC := examples/consumer/consumer.cpp
PRODUCER_BIN := $(EXAMPLES_DIR)/producer
CONSUMER_BIN := $(EXAMPLES_DIR)/consumer

.PHONY: all rust examples producer consumer clean check-cxxbridge

all: examples

rust:
	cargo build $(CARGO_FLAGS)

$(RUST_LIB): rust

check-cxxbridge: $(RUST_LIB)
	@test -n "$(CXXBRIDGE_INCLUDE)" || (echo "CXX bridge headers not found. Run 'make rust' first." >&2; exit 1)

$(EXAMPLES_DIR):
	mkdir -p $@

$(PRODUCER_BIN): $(PRODUCER_SRC) $(CPP_SOURCES) $(RUST_LIB) check-cxxbridge | $(EXAMPLES_DIR)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(PUBLIC_INCLUDE_DIR) -I$(CXXBRIDGE_INCLUDE) $(PRODUCER_SRC) $(CPP_SOURCES) $(RUST_LIB) $(LDFLAGS) -o $@

$(CONSUMER_BIN): $(CONSUMER_SRC) $(CPP_SOURCES) $(RUST_LIB) check-cxxbridge | $(EXAMPLES_DIR)
	$(CXX) $(CXXFLAGS) $(OPT_FLAGS) -I$(PUBLIC_INCLUDE_DIR) -I$(CXXBRIDGE_INCLUDE) $(CONSUMER_SRC) $(CPP_SOURCES) $(RUST_LIB) $(LDFLAGS) -o $@

examples: producer consumer

producer: $(PRODUCER_BIN)

consumer: $(CONSUMER_BIN)

clean:
	rm -f $(PRODUCER_BIN) $(CONSUMER_BIN)
