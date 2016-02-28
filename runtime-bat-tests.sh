
DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

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
