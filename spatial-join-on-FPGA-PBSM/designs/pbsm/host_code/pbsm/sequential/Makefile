# default CXX to g++
CXX_DEFAULT = g++
CXX_ALTERNATE = /opt/homebrew/Cellar/gcc/13.2.0/bin/g++-13

# check the alternate compiler
CHECK_CXX := $(shell command -v $(CXX_ALTERNATE) >/dev/null 2>&1 && echo $(CXX_ALTERNATE) || echo $(CXX_DEFAULT))

CXX = $(CHECK_CXX)

CXXFLAGS = -std=c++20 -O2 -I../common
TARGET = host

all: $(TARGET)

$(TARGET): main.cpp
	$(CXX) $(CXXFLAGS) $(EXTRA_CXXFLAGS) -o $(TARGET) main.cpp

# with and without opt
assembly: main.cpp
	$(CXX) -S $(CXXFLAGS) -o $(TARGET).s main.cpp
	$(CXX) -S $(CXXFLAGS) -O0 -o $(TARGET)_O0.s main.cpp

clean:
	rm -f $(TARGET) *.o

.PHONY: FORCE
	FORCE:
