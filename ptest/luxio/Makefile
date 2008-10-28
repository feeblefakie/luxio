prog=clean bput1 bget1 bput2 bget2 bput3 bget3 bput4 bget4 bput5 bget5 select
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

bput4: bput4.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

bget4: bget4.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

bput5: bput5.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

bget5: bget5.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

select: select.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

#bench-nocluster: bench-nocluster.cpp btree.h data.h
#	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

clean:
	rm -rf $(prog) *.bidx *.data *.aidx
