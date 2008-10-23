LDFLAGS=-L/usr/local/lib -lgtest -fprofile-arcs -ftest-coverage #-DDEBUG 
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

check:
	./btree_cluster_test
	./btree_noncluster_linked_test
	./btree_noncluster_padded_test
	./data_test
	./array_test

clean:
	rm -rf $(progs) *.bidx *.aidx *.data

