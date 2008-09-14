all: clean test

test: test.cpp
	g++ test.cpp -L/usr/lib -ldb -o test -I/opt/local/include/db46 -L/opt/local/lib/db46 -ldb

clean:
	rm -f *.db test


