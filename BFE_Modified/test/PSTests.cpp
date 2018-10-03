#include "TestUtils.h"
//#include "PlaneSweep.h"

//#define TEST_DEBUG


using namespace std;
using namespace flockcommon;


TEST(PointOrdering,  PlaneSweep) {

    vector<Point> points;

    points.push_back(Point(12.00, 20.00));
    points.push_back(Point(0.00, 20.00));
    points.push_back(Point(8.00, 22.00));
    points.push_back(Point(10.00, 22.00));

    sort(points.begin(), points.end(), point_x_compare);

    ASSERT_TRUE(points[0] == Point(0.0, 20.0));
}


TEST(PlaneSweepBoxes,  PlaneSweep) {
    set<Point> points;
    double xBase = 10.10, yBase = 20.15;

    for (unsigned int i = 0; i < 15; ++i) {
        Point p;
        p.x = (xBase * i * 1.2);
        p.y = (yBase * i * 1.4);
        p.oid = (i);

        points.insert(p);
    }
}


TEST(BoxExpand, PlaneSweep) {
    Point p(0, 4), p1(1,5), p2(2,6), p3(3,7);

    PlaneBox box(p1);

    ASSERT_EQ(box.getNwe(), p1);
    ASSERT_EQ(box.getSea(), p1);

    box.addPoint(p);
    box.addPoint(p1);
    box.addPoint(p2);
    box.addPoint(p3);

    ASSERT_EQ(box.getNwe().x, 0);
    ASSERT_EQ(box.getNwe().y, 7);
    ASSERT_EQ(box.getSea().x, 3);
    ASSERT_EQ(box.getSea().y, 4);
}

TEST(BoxIntersect,  PlaneSweep) {

    Point p(2, 0), p1(0, 2), p2(1, 3), p3(3, 1);
    PlaneBox box1(p);

    box1.addPoint(p);

    // Test MBR expansion
    box1.addPoint(p1);
    ASSERT_TRUE(Point(0, 2) == box1.getNwe());
    ASSERT_TRUE(Point(2, 0) == box1.getSea());

    PlaneBox box2(p2);
    box2.addPoint(p2);
    box2.addPoint(p3);

    p.x = (0);
    p.y = (1);
    PlaneBox box3(p1);
    p1.x = (-3);
    p1.y = (3);
    box3.addPoint(p1);
    box3.addPoint(p);

    p1.x = (4);
    p1.y = (2);
    PlaneBox box4(p1);
    box4.addPoint(p1);
    box4.addPoint( Point(2.0, 4.0) );


//    cout << box1.getSea().serialize() << ,  " << box1.getNwe().serialize() << endl;
//    cout << box2.getSea().serialize() << ,  " << box2.getNwe().serialize() << endl;
//    cout << box3.getSea().serialize() << ,  " << box3.getNwe().serialize() << endl;
//    cout << box4.getSea().serialize() << ,  " << box4.getNwe().serialize() << endl;

    // Testing intersection of boxes (MBRs)
    ASSERT_TRUE(box1.intersectsWith(box3));
    ASSERT_FALSE(box2.intersectsWith(box3));
    ASSERT_TRUE(box1.intersectsWith(box4));


    ASSERT_TRUE(box1.intersectsWith(box2));
    ASSERT_TRUE(box2.intersectsWith(box1));
    ASSERT_TRUE(box2.intersectsWith(box4));
}


TEST(PlaneSweepFlock,  PlaneSweep) {

    set<Point, less<Point>> points = TestUtils::readTestFile();

    ASSERT_EQ(18, points.size());

    PlaneBox box(*points.find(Point(0,0, 133)), 0.6);

    for (auto i = points.begin(); i != points.end(); ++i) {
        box.addPoint(*i);
    }

//    box.pruneYAxis();

    ASSERT_LE(4, box.getPointCount());

    vector<PointSet> flocks = box.getValidClusters(2);

    ASSERT_EQ(flocks.size(), 2);
}

TEST(HashClusterisSubset,  PlaneSweep) {
    Point p1(10, 11, 1), p2(11, 12, 2), p3(14, 15, 3), p4(15, 19, 4);

    MurMurHash murMurHash;
    SpookHash spookHash;
    vector<Hash *> hashes;
    hashes.push_back(&murMurHash);
    hashes.push_back(&spookHash);

    HashCluster cluster(hashes);
    cluster.insert(p1);
    cluster.insert(p2);
    cluster.insert(p3);
//    cout << cluster << endl;

    HashCluster cluster2(hashes);
    cluster2.insert(p1);
    cluster2.insert(p2);
    cluster2.insert(p3);
    cluster2.insert(p4);
//    cout << cluster2 << endl;

    ASSERT_TRUE(cluster.isSubset(cluster2) == 1);


//    DEBUG
#ifdef TEST_DEBUG
    cout << "Cluster:   " << cluster;
    cout << "\tCluster 2: " << cluster2 << endl;

    bitset<SIGN_SIZE> res = cluster2.getSignature() & cluster.getSignature();
    cout << "C & C2: " << (res) << endl;
    cout << "RES & ~C2: " << (res & ~cluster2.getSignature()) << endl;
    cout << "C2 & ~RES: " << (cluster2.getSignature() & ~res) << endl;
    cout << "RES.ulong: " << res.to_ullong() << "\t C2.ullong: " << cluster2.getSignature().to_ullong();
#endif
}

TEST(PlaneBoxhasPoint,  PlaneSweep) {
    PlaneBox box(Point(0.0,0.0,1), 0.6);
    box.addPoint(Point(0,0,2));
    box.addPoint(Point(0,0,3));
    box.addPoint(Point(0,0,4));

    ASSERT_TRUE(box.hasPoint(Point(0,3,2)));
    ASSERT_TRUE(box.hasPoint(Point(0,2,3)));
    ASSERT_TRUE(box.hasPoint(Point(0,1,4)));
}

TEST(PlaneBoxIsMember, PlaneSweep) {
    PlaneBox box(Point(0.0,0.0,1), 0.6);
    box.addPoint(Point(0,0,2));
    box.addPoint(Point(0,0,3));
    box.addPoint(Point(0,0,4));

    ASSERT_EQ(box.getMembership().size(), 4);
    ASSERT_TRUE(box.isMember(1));
    ASSERT_TRUE(box.isMember(2));
    ASSERT_TRUE(box.isMember(3));
    ASSERT_TRUE(box.isMember(4));
//    for (int i = 1; i < 5; ++i) {
//        ASSERT_TRUE(box.isMember(i));
//    }
}
