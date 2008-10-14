all: clean test

test: test.cpp
	g++ -g $< -L/usr/lib -ldb -o $@ -I/opt/local/include/db46 -L/opt/local/lib/db46 -ldb -lpthread

clean:
	rm -rf *.db test test-long select-long *.dSYM
