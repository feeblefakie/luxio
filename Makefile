
all: clean main bench

main: main.cpp btree.h
	g++ -g $< -o $@

bench: bench.cpp btree.h
	g++ -g $< -o $@

check:
	./main

clean:
	rm -f main test bench benchdb
