CXX := g++
CXXFLAGS := -o2 -ltbb
OMPFLAGS := -fopenmp
DBGFLAGS := -g

# default rule
default: all

# non-phony targets
multithread: multithread.cpp
	$(CXX) multithread.cpp $(CXXFLAGS) $(OMPFLAGS) -o multithread

multithread_energy: multithread_energy.cpp
	$(CXX) multithread_energy.cpp $(CXXFLAGS) $(OMPFLAGS) -o multithread_energy

singlethread: singlethread.cpp
	$(CXX) singlethread.cpp $(CXXFLAGS) -o singlethread

FPGA_index_constructor: FPGA_index_constructor.cpp
	$(CXX) FPGA_index_constructor.cpp $(CXXFLAGS) -o FPGA_index_constructor 

1d_stripes: 1d_stripes.cpp
	$(CXX) 1d_stripes.cpp $(CXXFLAGS) -o 1d_stripes 

1d_multithread: 1d_multithread.cpp
	$(CXX) 1d_multithread.cpp $(CXXFLAGS) $(OMPFLAGS) -o 1d_multithread 

tile_join_microbench: tile_join_microbench.cpp
	$(CXX) tile_join_microbench.cpp $(CXXFLAGS) $(OMPFLAGS) -o tile_join_microbench 

# phony rules
.PHONY: all
all: multithread singlethread FPGA_index_constructor multithread_energy 1d_stripes 1d_multithread tile_join_microbench

.PHONY: clean
clean:
	@rm -f multithread multithread_energy singlethread FPGA_index_constructor 1d_stripes 1d_multithread tile_join_microbench
