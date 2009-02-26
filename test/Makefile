#LDFLAGS=-L/usr/local/lib -lgtest -fprofile-arcs -ftest-coverage -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 #-DDEBUG -DDEBUG_VINFO
LDFLAGS=-L/usr/local/lib -lgtest -L../.libs -lluxio -fprofile-arcs -ftest-coverage -D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 #-DDEBUG #-DDEBUG_VINFO
progs=btree_cluster_test \
	  btree_noncluster_padded_test \
	  btree_noncluster_linked_test \
	  data_test \
	  array_test \
	  btree_thread_test

all: clean $(progs)

btree_cluster_test: btree_cluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

btree_noncluster_padded_test: btree_noncluster_padded_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

btree_noncluster_linked_test: btree_noncluster_linked_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

data_test: data_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

array_test: array_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

mylibc: mylibc.c
	gcc -shared -fPIC -o mylibc.so $< -ldl

get: get.cpp
	g++ -g $< -o $@ $ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

getm: getm.cpp
	g++ -g $< -o $@ $ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

put: put.cpp
	g++ -g $< -o $@ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

putm: putm.cpp
	g++ -g $< -o $@ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

del: del.cpp
	g++ -g $< -o $@ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

info: info.cpp
	g++ -g $< -o $@ $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

easy-checker: get getm put putm del info

btree_thread_test: btree_thread_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

check:
	./btree_cluster_test
	./btree_noncluster_linked_test
	./btree_noncluster_padded_test
	./data_test
	./array_test

clean:
	rm -rf $(progs) *.bidx *.aidx *.data get getm put putm del info *.gcda *.gcno *.dSYM *.so

