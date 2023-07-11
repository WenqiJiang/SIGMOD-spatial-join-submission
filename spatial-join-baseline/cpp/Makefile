CXX := g++
CXXFLAGS := -o2 -fopenmp -ltbb
DBGFLAGS := -g

# default rule
default: all

# non-phony targets
multithread: multithread.cpp
	$(CXX) multithread.cpp $(CXXFLAGS) -o multithread

multithread_energy: multithread_energy.cpp
	$(CXX) multithread_energy.cpp $(CXXFLAGS) -o multithread_energy

singlethread: singlethread.cpp
	$(CXX) singlethread.cpp $(CXXFLAGS) -o singlethread

FPGA_index_constructor: FPGA_index_constructor.cpp
	$(CXX) FPGA_index_constructor.cpp -o FPGA_index_constructor 

# phony rules
.PHONY: all
all: multithread singlethread FPGA_index_constructor multithread_energy

.PHONY: clean
clean:
	@rm -f multithread multithread_energy singlethread FPGA_index_constructor