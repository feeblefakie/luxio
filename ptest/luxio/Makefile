prog=clean bput1 bget1 bput2 bget2 bput3 bget3
all: $(prog)

bput1: bput1.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

bget1: bget1.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

bput2: bput2.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

bget2: bget2.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

bput3: bput3.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

bget3: bget3.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

#bench-nocluster: bench-nocluster.cpp btree.h data.h
#	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

clean:
	rm -rf $(prog) *.bidx *.data *.aidx
