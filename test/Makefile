LDFLAGS=-L/usr/local/lib -lgtest -DDEBUG -fprofile-arcs -ftest-coverage
progs=btree_cluster_test

all: clean $(progs) 

btree_cluster_test: btree_cluster_test.cpp
	g++ -g $< -o $@ $(LDFLAGS)

check:
	./btree_cluster_test

clean:
	rm -rf $(progs) *.bidx *.data

