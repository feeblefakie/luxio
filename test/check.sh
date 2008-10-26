#!/bin/sh -x

export LD_LIBRARY_PATH=/usr/local/lib

date=`date +%Y%m%d`
cd /tmp
mkdir $date
cd $date

work="luxio"
svn co svn+ssh://brant.lan/svn/newbtree/branches/test_dev $work
cd $work/test
ln -sf /home/hiroyuki/data.txt .
make

./btree_cluster_test
./btree_noncluster_linked_test
./btree_noncluster_padded_test
./data_test
./array_test
