
prog=clean main bench bench-nocluster select select-nocluster ctest delete intbench keybench data_test data_write data_read data_update array_test array_test-nocluster bench-mt bench-read array-mt
all: $(prog)

main: main.cpp btree.h data.h 
	g++ -g $< -o $@

select: select.cpp btree.h data.h
	g++ -g $< -o $@

select-nocluster: select-nocluster.cpp btree.h data.h
	g++ -g $< -o $@ -lpthread

delete: delete.cpp btree.h
	g++ -g $< -o $@

bench: bench.cpp btree.h data.h
	g++ -g $< -o $@ -lpthread

bench-read: bench-read.cpp btree.h data.h
	g++ -g $< -o $@ -lpthread

bench-nocluster: bench-nocluster.cpp btree.h data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

bench-mt: bench-mt.cpp btree.h data.h
	g++ -g $< -o $@ -lpthread

ctest: ctest.cpp btree.h
	g++ -g $< -o $@

intbench: intbench.cpp btree.h
	g++ -g $< -o $@

keybench: keybench.cpp btree.h
	g++ -g $< -o $@

data_test: data_test.cpp data.h
	g++ -g $< -o $@

data_write: data_write.cpp data.h
	g++ -g $< -o $@

data_read: data_read.cpp data.h
	g++ -g $< -o $@

data_update: data_update.cpp data.h
	g++ -g $< -o $@

array_test: array_test.cpp array.h data.h
	g++ -g $< -o $@ -lpthread

array-mt: array-mt.cpp array.h data.h
	g++ -g $< -o $@ -lpthread

array_test-nocluster: array_test-nocluster.cpp array.h data.h
	g++ -g $< -o $@ -lpthread

check:
	./main

clean:
	rm -rf $(prog) benchdb intbenchdb keybenchdb datadb *.dSYM
