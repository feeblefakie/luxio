BDB_CPPLFAGS=-I/usr/local/include -I/opt/local/include/db46
BDB_LDFLAGS=-L/usr/local/lib -ldb -L/opt/local/lib/db46 -ldb -L/usr/lib -ldb

prog=clean \
	 luxio_btree_put1 luxio_btree_get1 \
	 luxio_btree_put2 luxio_btree_get2 \
	 luxio_btree_put3 luxio_btree_get3 \
	 luxio_btree_put4 luxio_btree_get4 \
	 luxio_btree_put5 luxio_btree_get5 \
	 bdb_btree_put1 bdb_btree_get1 \
	 select
all: $(prog)

luxio_btree_put1: luxio_btree_put1.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

luxio_btree_get1: luxio_btree_get1.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

luxio_btree_put2: luxio_btree_put2.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

luxio_btree_get2: luxio_btree_get2.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

luxio_btree_put3: luxio_btree_put3.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread

luxio_btree_get3: luxio_btree_get3.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread

luxio_btree_put4: luxio_btree_put4.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

luxio_btree_get4: luxio_btree_get4.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

luxio_btree_put5: luxio_btree_put5.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

luxio_btree_get5: luxio_btree_get5.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

select: select.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG

bdb_btree_put1: bdb_btree_put1.cpp
	g++ -g $< -o $@ $(BDB_CPPLFAGS) $(BDB_LDFLAGS)

bdb_btree_get1: bdb_btree_get1.cpp
	g++ -g $< -o $@ $(BDB_CPPLFAGS) $(BDB_LDFLAGS)

#bench-nocluster: bench-nocluster.cpp btree.h data.h
#	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

clean:
	rm -rf $(prog) *.bidx *.data *.aidx
