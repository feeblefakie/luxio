BDB_CPPFLAGS=-I/usr/local/include -I/opt/local/include/db46 -O2
BDB_LDFLAGS=-L/usr/local/lib -ldb -L/opt/local/lib/db46 -ldb -L/usr/lib -ldb
TC_CPPFLAGS=-I/opt/local/include
TC_LDFLAGS=-L/usr/local/lib -ltokyocabinet -L/opt/local/lib -ltokyocabinet 

prog=clean \
	 luxio_btree_put1 luxio_btree_get1 \
	 luxio_btree_put2 luxio_btree_get2 \
	 luxio_btree_put3 luxio_btree_get3 \
	 luxio_btree_put4 luxio_btree_get4 \
	 luxio_btree_put5 luxio_btree_get5 \
	 luxio_btree_put6 luxio_btree_get6 \
	 luxio_btree_put7 luxio_btree_get7 \
	 bdb_btree_put1 bdb_btree_get1 \
	 bdb_btree_put2 bdb_btree_get2 \
	 bdb_btree_put3 bdb_btree_get3 \
	 bdb_btree_put4 bdb_btree_get4 \
	 bdb_btree_put5 bdb_btree_get5 \
	 tc_btree_put1 tc_btree_get1 \
	 tc_btree_put2 tc_btree_get2 \
	 tc_btree_put3 tc_btree_get3 \
	 tc_btree_put4 tc_btree_get4 \
	 tc_btree_put5 tc_btree_get5 \
	 tc_btree_put6 tc_btree_get6 \
	 tc_btree_put7 tc_btree_get7 \
	 select \
	 get_random_data
all: $(prog)

luxio_btree_put1: luxio_btree_put1.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_get1: luxio_btree_get1.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_put2: luxio_btree_put2.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_get2: luxio_btree_get2.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_put3: luxio_btree_put3.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_get3: luxio_btree_get3.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -O2

luxio_btree_put4: luxio_btree_put4.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_get4: luxio_btree_get4.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_put5: luxio_btree_put5.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_get5: luxio_btree_get5.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_put6: luxio_btree_put6.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_get6: luxio_btree_get6.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_put7: luxio_btree_put7.cpp ../../btree.h ../../data.h 
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

luxio_btree_get7: luxio_btree_get7.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

select: select.cpp ../../btree.h ../../data.h
	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DDEBUG -O2

get_random_data: get_random_data.cpp
	g++ -g $< -o $@

bdb_btree_put1: bdb_btree_put1.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_get1: bdb_btree_get1.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_put2: bdb_btree_put2.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_get2: bdb_btree_get2.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_put3: bdb_btree_put3.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_get3: bdb_btree_get3.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_put4: bdb_btree_put4.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_get4: bdb_btree_get4.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_put5: bdb_btree_put5.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

bdb_btree_get5: bdb_btree_get5.cpp
	g++ -g $< -o $@ $(BDB_CPPFLAGS) $(BDB_LDFLAGS)

tc_btree_put1: tc_btree_put1.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get1: tc_btree_get1.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put2: tc_btree_put2.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get2: tc_btree_get2.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put3: tc_btree_put3.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get3: tc_btree_get3.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put4: tc_btree_put4.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get4: tc_btree_get4.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put5: tc_btree_put5.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get5: tc_btree_get5.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put6: tc_btree_put6.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get6: tc_btree_get6.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_put7: tc_btree_put7.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

tc_btree_get7: tc_btree_get7.cpp
	g++ -g $< -o $@ $(TC_CPPFLAGS) $(TC_LDFLAGS)

#bench-nocluster: bench-nocluster.cpp btree.h data.h
#	g++ -g $< -o $@ -lpthread -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64

clean:
	rm -rf $(prog) *.bidx *.data *.aidx bdb[1-7] tc[1-7]
