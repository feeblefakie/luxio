all: clean test

test: test.cpp
	g++ test.cpp -L/usr/local/lib -ltokyocabinet -o test -L/opt/local/lib -ltokyocabinet -I/opt/local/include

clean:
	rm -f *.bdb test


