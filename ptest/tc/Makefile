all: clean test

test: test.cpp
	g++ test.cpp -L/usr/local/lib -ltokyocabinet -o test

clean:
	rm -f *.bdb test


