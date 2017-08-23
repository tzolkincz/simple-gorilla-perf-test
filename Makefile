
G++ = g++
G++_FLAGS = -O3 -march=native
LD_FLAGS = -l gtest -l pthread -l benchmark -l gflags

OBJECTS = *.cpp 
TARGET = perf

all:
	$(G++) -o $(TARGET) $(OBJECTS) $(G++_FLAGS)  $(LD_FLAGS) && ./perf

clean:
	rm -f $(TARGET) $(OBJECTS)
                    
.PHONY: all clean