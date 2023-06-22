# git submodule update --init --recursive
# git pull --recurse-submodules
# cd deps/libpaxos
# git checkout master
# cd ../../
# 
mkdir -p build
cd build
# export CC=/usr/bin/gcc
# export CXX=/usr/bin/g++
# cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake -DCMAKE_BUILD_TYPE=Release ..
make
