
prog=clean main bench select ctest delete intbench keybench data_test data_write data_read data_update
all: $(prog)

main: main.cpp btree.h
	g++ -g $< -o $@

select: select.cpp btree.h
	g++ -g $< -o $@

delete: delete.cpp btree.h
	g++ -g $< -o $@

bench: bench.cpp btree.h
	g++ -g $< -o $@

ctest: ctest.cpp btree.h
	g++ -g $< -o $@

intbench: intbench.cpp btree.h
	g++ -g $< -o $@

keybench: keybench.cpp btree.h
	g++ -g $< -o $@

data_test: data_test.cpp data3.h
	g++ -g $< -o $@

data_write: data_write.cpp data3.h
	g++ -g $< -o $@

data_read: data_read.cpp data3.h
	g++ -g $< -o $@

data_update: data_update.cpp data3.h
	g++ -g $< -o $@

check:
	./main

clean:
	rm -f $(prog) test benchdb intbenchdb keybenchdb datadb
