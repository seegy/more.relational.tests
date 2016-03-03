
DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

echo "start tr employees"

echo "search"

lein run tr employee join -c 1000 >> outs-$DATE/tr-search.out
lein run tr employee join -c 2500 >> outs-$DATE/tr-search.out
lein run tr employee join -c 5000 >> outs-$DATE/tr-search.out
lein run tr employee join -c 10000 >> outs-$DATE/tr-search.out
lein run tr employee join -c 15000 >> outs-$DATE/tr-search.out
lein run tr employee join -c 20000 >> outs-$DATE/tr-search.out
lein run tr employee join -c 30000 >> outs-$DATE/tr-search.out



echo "finished tr employees"
