LDFLAGS=-L/usr/local/lib -lgtest -DDEBUG -fprofile-arcs -ftest-coverage
progs=btree_cluster_test btree_noncluster_padded_test btree_noncluster_linked_test

all: clean $(progs) check

btree_cluster_test: btree_cluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

btree_noncluster_padded_test: btree_noncluster_padded_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

btree_noncluster_linked_test: btree_noncluster_linked_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

check:
	./btree_cluster_test
	./btree_noncluster_padded_test
	./btree_noncluster_linked_test

clean:
	rm -rf $(progs) *.bidx *.data

