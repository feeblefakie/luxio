all: clean test test-long select-long

test: test.cpp
	g++ -g $< -L/usr/lib -ldb -o $@ -I/opt/local/include/db46 -L/opt/local/lib/db46 -ldb

test-long: test-long.cpp
	g++ -g $< -L/usr/lib -ldb -o $@ -I/opt/local/include/db46 -L/opt/local/lib/db46 -ldb

select-long: select-long.cpp
	g++ -g $< -L/usr/lib -ldb -o $@ -I/opt/local/include/db46 -L/opt/local/lib/db46 -ldb

clean:
	rm -rf *.db test test-long select-long *.dSYM
