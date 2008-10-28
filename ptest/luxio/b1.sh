#!/bin/sh

dbname_base="b1"
num=1000000

i=0
while [ $i -lt 10 ];
do
    dbname="$dbname_base-$i"
    ./bput1 $dbname $num
    i=`expr $i + 1`
done

i=0
while [ $i -lt 10 ];
do
    dbname="$dbname_base-$i"
    ./bget1 $dbname $num 1
    i=`expr $i + 1`
done
