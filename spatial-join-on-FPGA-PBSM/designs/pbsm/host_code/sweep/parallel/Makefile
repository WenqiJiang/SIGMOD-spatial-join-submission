# default
CXX := g++
CXX_ALTERNATE := /opt/homebrew/Cellar/gcc/13.2.0/bin/g++-13

# check if CXX_ALTERNATE is available
CXX := $(shell command -v $(CXX_ALTERNATE) >/dev/null 2>&1 && echo $(CXX_ALTERNATE) || echo $(CXX))

CXXFLAGS := -std=c++20 -O2 -I../common -fopenmp

TARGET = host

all: $(TARGET)

$(TARGET): 1d_multithread.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_CXXFLAGS) -o $(TARGET) 1d_multithread.cpp

clean:
	rm -f $(TARGET) *.o

.PHONY: FORCE
FORCE: