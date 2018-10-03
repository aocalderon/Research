#include <common/Cluster.h>
//#include <common/InvertedIndex.hpp>
#include "TestUtils.h"


TEST(Point, PointEqual) {
    Point p(10.00, 20.00);
    Point p2(10.00, 20.00);
    Point p3(10.00, 22.00);
    ASSERT_EQ(p, p2);
    ASSERT_FALSE(p == p3);
}


TEST(Point, PointDistance) {
    Point p(0, 0), p1(4, 3);

    ASSERT_EQ(p.distance(p1), 5);
}


TEST(PointSet, PointSetContain) {

    Point p(0, 0), p1(4, 3), p2(4, 4);
    p2.oid = 14;
    p.oid = 10;
    p1.oid = 11;
    p.x = 10;

    PointSet ps;
    ps.insert(p);
    ps.insert(p1);

    PointSet ps2;
    ps2.insert(p);
    ps2.insert(p1);
    ps2.insert(p2);

     ASSERT_TRUE(ps.hasPoint(p));
     ASSERT_TRUE(ps.hasPoint(p1));
     ASSERT_TRUE(ps2.contains(ps));
}


TEST(Cluster, ClusterinRange) {

    set<Point, less<Point>> points = TestUtils::readTestFile();

    Point center1(948.94, 982.635);

    PointSet cluster;
    cluster.centerX = points.begin()->x;
    cluster.centerY = points.begin()->y;
    cluster.radius = 0.3;

    ASSERT_EQ(18, points.size());

    int inRange = 0;

    for (auto i = points.begin(); i != points.end(); ++i) {
        if (i->oid != 73) {
//            cout << "Distance: " << center1.distance(*i) << " to point #" << i->oid << endl;
            if (cluster.inRange(*i, DistanceMetric::L2)) inRange++;
        }
    }

    ASSERT_EQ(6, inRange);
}


TEST(Common, InvIndex) {
    // template<class TKey, class TDoc, class TDocId>

    Cluster cl1;
    cl1.points.insert(Point(0,0,1));
    cl1.points.insert(Point(0,0,2));
    cl1.points.insert(Point(0,0,3));

    Cluster cl2;
    cl2.points.insert(Point(0,0,2));
    cl2.points.insert(Point(0,0,3));
    cl2.points.insert(Point(0,0,4));

    InvertedIndex<int,Cluster,long> myIdx = InvertedIndex<int, Cluster, long>();
    myIdx.insertDocument(cl1, 1);
    myIdx.insertDocument(cl2, 2);

    set<int> dcs = {1, 3};
    set<long> ids = myIdx.andQuery(dcs);

     ASSERT_EQ(ids.size(), 1);

    ids.clear();
    dcs = {2, 3};
    ids = myIdx.andQuery(dcs);

    ASSERT_EQ(ids.size(), 2);
}
