
DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

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
