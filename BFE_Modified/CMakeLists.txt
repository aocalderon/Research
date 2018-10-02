cmake_minimum_required(VERSION 3.1)
project(flocks CXX C)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -ggdb -Wall -Wno-delete-non-virtual-dtor")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

option(test "Build all tests" OFF)


include_directories(
        src/arboretum/
        src/arboretum/include
        src/include
        src/include/lib
        src/include/psb
        src/include/common
        src/include/hash
        src/include/hash/functions
        src/
)

set(BASE_SRC_FILES
    src/include/hash/Hash.h
    src/include/common/definitions.hpp
    src/include/common/InvertedIndex.hpp
    src/include/common/Cluster.cpp
    src/include/common/CenterPair.cpp
    src/include/common/Center.cpp
    src/include/common/FlockAnswer.cpp
    src/include/common/Point.cpp
    src/include/common/PointPair.cpp
    src/include/common/PointSet.cpp
    src/include/hash/functions/murmur3.c
    src/include/hash/functions/SpookyV2.cpp
    src/include/hash/MurMurHash.cpp
    src/include/hash/SpookHash.cpp
    src/include/psb/Hashable.hpp
    src/include/psb/HashCluster.cpp
    src/include/psb/HashPlaneBox.cpp
    src/include/GridIndex.h
    src/BasicFlock.cpp
    src/FlockBase.cpp
    src/FlockRunner.cpp
    src/include/psb/PlaneBox.cpp
    src/include/psb/PlaneSweep.cpp)

set(SOURCE_FILES
        ${BASE_SRC_FILES}
        src/flocks.cpp)

add_library(flocks_lib ${BASE_SRC_FILES})
add_executable(flocks ${SOURCE_FILES})

target_link_libraries(flocks flocks_lib)

#if (test)
    link_directories(
            src/include/lib/googletest/src
    )

    add_subdirectory(src/include/lib/googletest)

    enable_testing()

    include_directories(src/include/lib/googletest
            src/include/lib/googletest/include)

    set(TEST_FILES
            test/TestUtils.h
            test/TestUtils.cpp)

    add_library(tests_lib ${TEST_FILES})


    add_executable(CheckClusters ${BASE_SRC_FILES} test/CheckClusters.cpp)

    file(GLOB ALL_TEST_FILES
        test/*Tests.cpp
    )
    list(SORT ALL_TEST_FILES)

    set(ALL_GRID_TEST_FILES
        test/BasicTests.cpp
        test/BFETests.cpp
        test/CommonTests.cpp)

    add_executable(AllGridTests ${ALL_GRID_TEST_FILES})
    target_link_libraries(AllGridTests gtest gtest_main)
    target_link_libraries(AllGridTests flocks_lib)
    target_link_libraries(AllGridTests tests_lib)

    set(ALL_PS_TEST_FILES
        test/PSITests.cpp
        test/PSTests.cpp)

    add_executable(AllPSTests ${ALL_PS_TEST_FILES})
    target_link_libraries(AllPSTests gtest gtest_main)
    target_link_libraries(AllPSTests flocks_lib)
    target_link_libraries(AllPSTests tests_lib)

    add_executable(AllGoogleTests ${ALL_TEST_FILES})
    target_link_libraries(AllGoogleTests gtest gtest_main)
    target_link_libraries(AllGoogleTests flocks_lib)
    target_link_libraries(AllGoogleTests tests_lib)

    add_executable(BasicTests ${TEST_FILES} test/BasicTests.cpp)
    target_link_libraries(BasicTests gtest gtest_main)
    target_link_libraries(BasicTests flocks_lib)

    add_executable(BFEBusesTests ${TEST_FILES} test/BFETests.cpp)
    target_link_libraries(BFEBusesTests gtest gtest_main)
    target_link_libraries(BFEBusesTests flocks_lib)

    add_executable(PSIBusesTests ${TEST_FILES} test/PSITests.cpp)
    target_link_libraries(PSIBusesTests gtest gtest_main)
    target_link_libraries(PSIBusesTests flocks_lib)

    add_executable(CommomTests ${TEST_FILES} test/CommonTests.cpp)
    target_link_libraries(CommomTests gtest gtest_main)
    target_link_libraries(CommomTests flocks_lib)

    add_executable(PSTests ${TEST_FILES} test/PSTests.cpp)
    target_link_libraries(PSTests gtest gtest_main)
    target_link_libraries(PSTests flocks_lib)
#endif ()
