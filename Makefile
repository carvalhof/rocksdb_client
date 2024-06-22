#!/bin/bash

TARGET = client

CXX = g++
CXXFLAGS = -Wall -Wextra -Werror -pedantic -pedantic-errors \
         -Wunused -Wuninitialized -Wfloat-equal -Wshadow \
         -O2 -fstack-protector-strong -fsanitize=address \
         -pthread -std=c++17 -I${HOME}/rocksdb/include
LDFLAGS = -L${HOME}/rocksdb -lrocksdb -lpthread -lsnappy -lz -lbz2 -llz4 -lzstd -ldl -fsanitize=address

SRCS = main.cpp

OBJS = $(SRCS:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)
