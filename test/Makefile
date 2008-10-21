LDFLAGS=-L/usr/local/lib -lgtest -DDEBUG -fprofile-arcs -ftest-coverage
progs=btree_cluster_test btree_noncluster_test

all: clean $(progs) check

btree_cluster_test: btree_cluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

btree_noncluster_test: btree_noncluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS) -lpthread

check:
	./btree_cluster_test
	./btree_noncluster_test

clean:
	rm -rf $(progs) *.bidx *.data

