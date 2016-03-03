#!/bin/bash





DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

echo "start hashrel employees"
echo "manipulation"

lein run hashrel employee manipulation -c 1000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 2500  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 5000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 10000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 15000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 20000  >> outs-$DATE/hashrel-manipulation.out
lein run hashrel employee manipulation -c 30000  >> outs-$DATE/hashrel-manipulation.out

echo "finished hashrel employees"


echo "start bat employees"
echo "manipulation"

lein run bat employee manipulation -c 1000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 2500  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 5000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 10000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 15000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 20000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 30000  >> outs-$DATE/bat-manipulation.out

echo "finished bat employees"

echo "start tr employees"
echo "manipulation"

lein run tr employee manipulation -c 1000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 2500  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 5000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 10000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 15000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 20000  >> outs-$DATE/tr-manipulation.out
lein run tr employee manipulation -c 30000  >> outs-$DATE/tr-manipulation.out

echo "finished tr employees"
