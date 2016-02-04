current=${PWD}

if [ ! -d ../more.relational ]
  then
    echo Oh, no more.relational there! I will download it for you.
    cd ..
    git clone https://github.com/seegy/more.relational.git
    cd $current
    echo Now you have it.
fi

echo Installing more.relational with lein...
cd ../more.relational
lein install
echo Done installing.

cd $current

lein test
