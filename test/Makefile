LDFLAGS=-L/usr/local/lib -lgtest -fprofile-arcs -ftest-coverage #-DDEBUG #-DDEBUG_VINFO
progs=btree_cluster_test \
	  btree_noncluster_padded_test \
	  btree_noncluster_linked_test \
	  data_test \
	  array_test

all: clean $(progs)

btree_cluster_test: btree_cluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

btree_noncluster_padded_test: btree_noncluster_padded_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

btree_noncluster_linked_test: btree_noncluster_linked_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

data_test: data_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

array_test: array_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

mylibc: mylibc.c
	gcc -shared -fPIC -o mylibc.so $< -ldl

get: get.cpp
	g++ -g $< -o $@ $ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

getm: getm.cpp
	g++ -g $< -o $@ $ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

put: put.cpp
	g++ -g $< -o $@ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

putm: putm.cpp
	g++ -g $< -o $@ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

del: del.cpp
	g++ -g $< -o $@ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

info: info.cpp
	g++ -g $< -o $@ -lpthread $(LDFLAGS) -DDEBUG -DDEBUG_VINFO

easy-checker: get getm put putm del info

check:
	./btree_cluster_test
	./btree_noncluster_linked_test
	./btree_noncluster_padded_test
	./data_test
	./array_test

clean:
	rm -rf $(progs) *.bidx *.aidx *.data get getm put putm del info *.gcda *.gcno *.dSYM *.so

