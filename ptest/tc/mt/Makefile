all: clean test

test: test.cpp
	g++ -g $< -L/usr/local/lib -ltokyocabinet -o $@ -L/opt/local/lib -ltokyocabinet -I/opt/local/include -lpthread

clean:
	rm -rf *.bdb test test-long select-long *.dSYM
