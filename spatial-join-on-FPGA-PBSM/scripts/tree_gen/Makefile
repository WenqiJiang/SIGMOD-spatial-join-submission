CXX := g++
CXXFLAGS := -O2
OMPFLAGS := -fopenmp
DBGFLAGS := -g
CFLAGS=$(CXXFLAGS) -std=c++20

# default rule
default: all

tree_gen: tree_gen.cpp
	$(CXX) tree_gen.cpp $(CFLAGS) -o tree_gen

# phony rules
.PHONY: all
all: tree_gen

.PHONY: clean
clean:
	@rm -f tree_gen
