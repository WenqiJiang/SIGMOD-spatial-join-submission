#CXX := g++
CXX := /opt/homebrew/Cellar/gcc/13.2.0/bin/g++-13
CXXFLAGS := -std=c++17 -o2 -I../common

TARGET = host

all: $(TARGET)

$(TARGET): 1d_stripes.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_CXXFLAGS) -o $(TARGET) 1d_stripes.cpp

clean:
	rm -f $(TARGET) *.o

.PHONY: FORCE
	FORCE:
