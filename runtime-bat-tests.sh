
DATE=`date +%Y-%m-%d-%T`
mkdir outs-$DATE

echo "start bat employees"
echo "create"

lein run bat employee creating -c 1000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 2500 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 5000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 10000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 15000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 20000 >> outs-$DATE/bat-create.out
lein run bat employee creating -c 30000 >> outs-$DATE/bat-create.out

echo "manipulation"

lein run bat employee manipulation -c 1000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 2500  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 5000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 10000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 15000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 20000  >> outs-$DATE/bat-manipulation.out
lein run bat employee manipulation -c 30000  >> outs-$DATE/bat-manipulation.out

echo "finished bat employees"
