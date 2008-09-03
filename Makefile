
all: clean main

main: main.cpp btree.h
	g++ -g $< -o $@

check:
	./main

clean:
	rm -f main test
