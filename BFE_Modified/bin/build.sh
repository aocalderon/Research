#!/bin/bash

[ ! -d build ] && mkdir build
cmake -DCMAKE_BUILD_TYPE=Release . -B./build

#cmake --build build --clean-first -- -j 8
#cmake --build build --target flocks --clean-first -- -j 8
cmake --build build $@ -- -j 8


