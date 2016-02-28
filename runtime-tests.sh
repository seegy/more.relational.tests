#!/bin/bash

COUNTER=5
until [  $COUNTER -lt 0 ]; do

echo "run nr." $COUNTER
let COUNTER-=1


DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

echo "start hashrel employees"
echo "create"

lein run hashrel employee creating -c 10000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 25000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 50000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 100000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 150000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 200000 >> outs-$DATE/hashrel-create.out
lein run hashrel employee creating -c 300024 >> outs-$DATE/hashrel-create.out

echo "search"

lein run hashrel employee search -c 10000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 25000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 50000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 100000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 150000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 200000 >> outs-$DATE/hashrel-search.out
lein run hashrel employee search -c 300024 >> outs-$DATE/hashrel-search.out

echo "join"

lein run hashrel employee join -c 10000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 25000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 50000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 100000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 150000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 200000 >> outs-$DATE/hashrel-join.out
lein run hashrel employee join -c 300024 >> outs-$DATE/hashrel-join.out

echo "manipulation"

lein run hashrel employee manipulation -c 10000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 25000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 50000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 100000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 150000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 200000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 300024  >> outs-$DATE/hashrel-manipulation.out

echo "finished hashrel employees"


echo "start bat employees"
echo "create"

lein run bat employee creating -c 10000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 25000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 50000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 100000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 150000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 200000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 300024 >> outs-$DATE/bat-create.out

echo "search"

lein run bat employee search -c 10000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 25000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 50000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 100000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 150000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 200000 >> outs-$DATE/bat-search.out
lein run bat employee search -c 300024 >> outs-$DATE/bat-search.out

echo "join"

lein run bat employee join -c 10000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 25000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 50000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 100000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 150000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 200000 >> outs-$DATE/bat-join.out
lein run bat employee join -c 300024 >> outs-$DATE/bat-join.out

echo "manipulation"

lein run bat employee manipulation -c 10000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 25000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 50000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 100000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 150000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 200000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 300024  >> outs-$DATE/bat-manipulation.out

echo "finished bat employees"

echo "start tr employees"
echo "create"

lein run tr employee creating -c 10000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 25000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 50000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 100000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 150000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 200000 >> outs-$DATE/tr-create.out
lein run tr employee creating -c 300024 >> outs-$DATE/tr-create.out

echo "search"

lein run tr employee search -c 10000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 25000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 50000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 100000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 150000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 200000 >> outs-$DATE/tr-search.out
lein run tr employee search -c 300024 >> outs-$DATE/tr-search.out

echo "join"

lein run tr employee join -c 10000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 25000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 50000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 100000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 150000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 200000 >> outs-$DATE/tr-join.out
lein run tr employee join -c 300024 >> outs-$DATE/tr-join.out

echo "manipulation"

lein run tr employee manipulation -c 10000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 25000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 50000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 100000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 150000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 200000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 300024  >> outs-$DATE/tr-manipulation.out

echo "finished tr employees"

done
