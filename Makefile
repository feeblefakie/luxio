
all: clean main bench select

main: main.cpp btree.h
	g++ -g $< -o $@

select: select.cpp btree.h
	g++ -g $< -o $@

bench: bench.cpp btree.h
	g++ -g $< -o $@

check:
	./main

clean:
	rm -f main test bench benchdb select
