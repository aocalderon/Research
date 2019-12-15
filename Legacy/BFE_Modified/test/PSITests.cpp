//
// Created by denis on 18/05/16.
//

#include <BasicFlock.h>
#include "TestUtils.h"

TEST(PSIBusesTests, VaringNumElements) {

    unsigned int eTotalAnswers[5]  = {777, 17, 0, 0, 0};
    unsigned int eTotalCenters[5]  = {126454, 61928, 21642, 5720, 2336};
    unsigned int eTotalClusters[5] = {6364, 636, 66, 9, 7};
    unsigned int eTotalPairs[5]    = {64533, 31594, 11062, 2961, 1209};

    for (unsigned short numElements = MIN_NUM, i = 0; numElements <= MAX_NUM;
         numElements += INC_NUM, ++i) {

        BasicFlock *algorithm = new BasicFlock(numElements, DEF_DELTA, DEF_DIST, DATA_FILE);
        algorithm->start(BFE_PSB_IVIDX_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters[i],  algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters[i], algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs[i],    algorithm->getTotalPairs());

        delete algorithm;
    }
}

TEST(PSIBusesTests, VaringDist) {

    unsigned int eTotalAnswers[4]  = {36, 214, 520, 1166};
    unsigned int eTotalCenters[4]  = {40130, 84200, 136092, 197786};
    unsigned int eTotalClusters[4] = {1203, 2676, 4581, 6989};
    unsigned int eTotalPairs[4]    = {20760, 43099, 69295, 100309};

    unsigned short i = 0;

    for (float dist = 0.4; dist <= 1.1; dist += 0.2, ++i) {

        BasicFlock *algorithm = new BasicFlock(DEF_NUM, DEF_DELTA, dist, DATA_FILE);
        algorithm->start(BFE_PSB_IVIDX_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters[i],  algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters[i], algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs[i],    algorithm->getTotalPairs());

        delete algorithm;
    }
}

TEST(PSIBusesTests, VaringLen) {

    unsigned int eTotalAnswers[5]  = {1244, 319, 129, 72, 34};
    unsigned int eTotalCenters     = 108666;
    unsigned int eTotalClusters    = 3517;
    unsigned int eTotalPairs       = 55460;

    for (unsigned short len = MIN_DELTA, i = 0; len <= MAX_DELTA; len += INC_DELTA, ++i) {

        BasicFlock *algorithm = new BasicFlock(DEF_NUM, len, DEF_DIST, DATA_FILE);
        algorithm->start(BFE_PSB_IVIDX_METHOD);
        cout << endl;

        EXPECT_EQ(eTotalAnswers[i],  algorithm->getTotalAnswers());
        EXPECT_EQ(eTotalCenters,     algorithm->getTotalCenters());
        EXPECT_EQ(eTotalClusters,    algorithm->getTotalClusters());
        EXPECT_EQ(eTotalPairs,       algorithm->getTotalPairs());

        delete algorithm;
    }
}
