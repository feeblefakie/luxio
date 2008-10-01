all: clean test test-long select-long

test: test.cpp
	g++ -g $< -L/usr/local/lib -ltokyocabinet -o $@ -L/opt/local/lib -ltokyocabinet -I/opt/local/include

test-long: test-long.cpp
	g++ -g $< -L/usr/local/lib -ltokyocabinet -o $@ -L/opt/local/lib -ltokyocabinet -I/opt/local/include

select-long: select-long.cpp
	g++ -g $< -L/usr/local/lib -ltokyocabinet -o $@ -L/opt/local/lib -ltokyocabinet -I/opt/local/include

clean:
	rm -rf *.bdb test test-long select-long *.dSYM
