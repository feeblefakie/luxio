CPPLFAGS=-I/usr/local/include -I/opt/local/include/db46
LDFLAGS=-L/usr/local/lib -ldb -L/opt/local/lib/db46 -ldb -L/usr/lib -ldb

all: clean test test-long select-long

test: test.cpp
	g++ -g $< -o $@ $(LDFLAGS) $(CPPFLAGS)

test-long: test-long.cpp
	g++ -g $< -o $@ $(LDFLAGS) $(CPPFLAGS)

select-long: select-long.cpp
	g++ -g $< -o $@ $(LDFLAGS) $(CPPFLAGS)

clean:
	rm -rf *.db test test-long select-long *.dSYM
