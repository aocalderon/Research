//
// Created by denis on 18/05/16.
//

#include <BasicFlock.h>
#include "TestUtils.h"

TEST(BFEBusesTests, VaringNumElements) {

    unsigned int eTotalAnswers[5]  = {787, 17, 0, 0, 0};
    unsigned int eTotalCenters[5]  = {122372, 55568, 18674, 5110, 2136};
    unsigned int eTotalClusters[5] = {6369, 641, 67, 9, 7};
    unsigned int eTotalPairs[5]    = {62473, 28380, 9573, 2652, 1109};

    for (unsigned short numElements = MIN_NUM, i = 0; numElements <= MAX_NUM;
         numElements += INC_NUM, ++i) {

        BasicFlock *algorithm = new BasicFlock(numElements, DEF_DELTA, DEF_DIST, DATA_FILE);
        algorithm->start(BFE_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters[i],  algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters[i], algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs[i],    algorithm->getTotalPairs());

        delete algorithm;
    }
}

TEST(BFEBusesTests, VaringDist) {

    unsigned int eTotalAnswers[4]  = {36, 214, 521, 1181};
    unsigned int eTotalCenters[4]  = {36978, 79416, 129884, 189588};
    unsigned int eTotalClusters[4] = {1203, 2678, 4587, 6998};
    unsigned int eTotalPairs[4]    = {19147, 40667, 66140, 96159};

    unsigned short i = 0;

    for (float dist = 0.4; dist <= 1.1; dist += 0.2, ++i) {

        BasicFlock *algorithm = new BasicFlock(DEF_NUM, DEF_DELTA, dist, DATA_FILE);
        algorithm->start(BFE_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters[i],  algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters[i], algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs[i],    algorithm->getTotalPairs());

        delete algorithm;
    }
}

TEST(BFEBusesTests, VaringLen) {

    unsigned int eTotalAnswers[5]  = {1247, 319, 129, 72, 34};
    unsigned int eTotalCenters     = 103522;
    unsigned int eTotalClusters    = 3523;
    unsigned int eTotalPairs       = 52862;

    for (unsigned short len = MIN_DELTA, i = 0; len <= MAX_DELTA; len += INC_DELTA, ++i) {

        BasicFlock *algorithm = new BasicFlock(DEF_NUM, len, DEF_DIST, DATA_FILE);
        algorithm->start(BFE_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters,     algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters,    algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs,       algorithm->getTotalPairs());

        delete algorithm;
    }
}
