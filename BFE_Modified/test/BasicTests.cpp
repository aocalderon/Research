//
// Created by denis on 05/07/16.
//

#include "TestUtils.h"
//#include "BasicFlock.h"

TEST(BasicTests, ReadingPtsFromDB) {

    // Caribous DB test:
    BasicTests *bfe = new BasicTests("../flocks/data/caribou_by_time.txt");
    bfe->testOpenStream();

    long linesTS0 = bfe->testCountLinesTS0();
    bfe->testGoToFinBegin();

    bfe->testLoadLocations();
    EXPECT_EQ(linesTS0, bfe->testGetMyPoints().size());

    delete bfe;


    // Buses DB test:
    bfe = new BasicTests("../flocks/data/buses_by_time.txt");
    bfe->testOpenStream();

    linesTS0 = bfe->testCountLinesTS0();

    bfe->testGoToFinBegin();

    bfe->testLoadLocations();
    EXPECT_EQ(linesTS0, bfe->testGetMyPoints().size());
    delete bfe;
    cout << endl;
}
