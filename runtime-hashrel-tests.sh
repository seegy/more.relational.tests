
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
