CXX := g++
CFLAGS := -std=c++17 -I/usr/include -I./ -lrt -lpthread -O3 -Wall -Wextra -march=native -Wall -Wextra -Wshadow  -fno-builtin-memcpy -fno-builtin-memmove -fno-builtin-memcmp -DNDEBUG -g3 -O3 -flto -fno-stack-protector  -Wno-unknown-pragmas  -rdynamic -lm -lrt -g

DIRS=./benchmark/build
$(shell mkdir -p $(DIRS))

DTS:
	$(CXX) $(CFLAGS) -w -o $(DIRS)/benchmark benchmark/main.cpp -lpthread -DSEP

DTS_noSEP:
	$(CXX) $(CFLAGS) -w -o $(DIRS)/benchmark benchmark/main.cpp -lpthread


# Customized-YCSB
DTS_CUST_YCSB:
	$(CXX) $(CFLAGS) -w -o $(DIRS)/benchmark benchmark/ycsb_style_main.cpp -lpthread -DSEP

DTS_noSEP_CUST_YCSB:
	$(CXX) $(CFLAGS) -w -o $(DIRS)/benchmark benchmark/ycsb_style_main.cpp -lpthread

all:
	echo "NOTHING YET"

clean:
	rm -rf src/*.o build/* util/*.o external/*.o
