/*
Flock Evaluation Algorithms

Copyright (C) 2009 Marcos R. Vieira
Copyright (C) 2015, 2016 Pedro S. Tanaka
Copyright (C) 2016 Denis E. Sanches

This program is free software: you can redistribute it and/or modify
        it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
        but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __GRIDINDEX__
#define __GRIDINDEX__


//#define DOUBLE_EQ(x,y) ((x >= (y - EPSILON)) && (x <= (y + EPSILON)))
//#define DOUBLE_NEQ(x,y) ((x < (y - EPSILON)) || (x > (y + EPSILON)))
//#define DOUBLE_LESS(x,y) ((y - x) > EPSILON)
#define MY_EPSILON 0.001
#define DOUBLE_EQ(x, y)  (fabs(double(x - y)) < MY_EPSILON)
#define DOUBLE_NEQ(x, y) (!DOUBLE_EQ(x, y))
#define DOUBLE_LES(x, y) (x < y)
#define DOUBLE_LEQ(x, y) ((x < y) || DOUBLE_EQ(x,y))
#define DOUBLE_GRT(x, y) (x >  y)
#define DOUBLE_GRE(x, y) ((x > y) || DOUBLE_EQ(x,y))

//#define __DEBUGSTEPS__
//#define __DEBUGPRINT__
//#define __MAXTIME__
//#define MAXTIME 200
//#define __DEBUGMODE__
#define INCFACTOR 1
//#define __DEBUGCENTER__
#define __RADIUSPRUNING__

// (double)CLOCKS_PER_SEC
#define CLOCK_DIV 1000.0
//#define __PRINTTIMESTATUS
#define MIN_GRID 900
#define MAX_GRID 1000

#include <cstring>


using namespace std;
using namespace flockcommon;

//------------------------------------------------------------------------------
class GridIndex {
public:
    GridIndex(double min_x, double max_x, double min_y, double max_y,
              unsigned int max_oid, set<Point, less<Point> > pnts,
              double min_dist, int min_pnts) {

        set<Point, less<Point> >::iterator it;
        set<Point, less<Point> >::iterator it2;
        int x, y, nearest;
        double tmp, dist;

        minPoint.x = min_x;
        minPoint.y = min_y;
        maxPoint.x = max_x;
        maxPoint.y = max_y;
        mySize = max_oid;
        minDist = min_dist;
        nroFlocks = min_pnts;
        sizeGridX = int(ceil((max_x - min_x) / minDist)) + 1;
        sizeGridY = int(ceil((max_y - min_y) / minDist)) + 1;
        // allocate memory for the grid structure.
        points = new set<Point, less<Point> > *[sizeGridX];

        for (unsigned int w = 0; w < sizeGridX; w++) {
            points[w] = new set<Point, less<Point> >[sizeGridY];
        }//end for

#ifdef __USEBITOPER__
		bits = new bool * [size()];
		for (int w=0; w<size(); w++){
			bits[w] = new bool[size()];
			memset(bits[w], 0, sizeof(bool)*size());
		}//end for
#endif

        valid = new bool[size()];
        memset(valid, 1, sizeof(bool) * size());
        counts = new int[size()];
        memset(counts, 0, sizeof(int) * size());

        // add the point
        for (it = pnts.begin(); it != pnts.end(); ++it) {
            //////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////
            nearest = 0;

            for (it2 = pnts.begin(); it2 != pnts.end(); ++it2) {
                tmp = (*it).x - (*it2).x;
                dist = tmp * tmp;
                tmp = (*it).y - (*it2).y;
                dist += tmp * tmp;
                dist = sqrt(dist);

                if (DOUBLE_LEQ(dist, minDist)) {
#ifdef __USEBITOPER__
					bits[(*it).oid][(*it2).oid] = true;
#endif
                    nearest++;
                }//end if
            }//end for
            //////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////
            if (nearest >= nroFlocks) {
                counts[(*it).oid] = nearest;
                x = gridX(*it);
                y = gridY(*it);
                points[x][y].insert(*it); //insert the point in the grid.
            }//end if
        }//end for
    }//end

    ~GridIndex() {
        for (unsigned int x = 0; x < sizeGridX; x++) {
            delete[] points[x];
        }

#ifdef __USEBITOPER__
		for (int w=0; w<size(); w++)
			delete[] bits[w];
		delete[] bits;
#endif
        delete[] points;
        delete[] counts;
        delete[] valid;
    }//end

    bool qualify(set<Point, less<Point> >::iterator it1,
                 set<Point, less<Point> >::iterator it2) {
        // first test if they have each other.
        if (valid[(*it1).oid] && valid[(*it2).oid]) {
#ifdef __USEBITOPER__
			if (bits[(*it1).oid][(*it2).oid] && bits[(*it2).oid][(*it1).oid]){ // one has the other.
				int count = 0;
				for (int i=0; i<size(); i++){
					if (bits[(*it1).oid][i] & bits[(*it2).oid][i]) count++;
				}//end for
				return (count >= nroFlocks);
			}//end if
#else
            // check to see if they are together.
            double tmp, dist;
            tmp = (*it1).x - (*it2).x;
            dist = tmp * tmp;
            tmp = (*it1).y - (*it2).y;
            dist += tmp * tmp;
            dist = sqrt(dist);

            return DOUBLE_LEQ(dist, minDist);
#endif
        }//end if
        return false;
    }//end

    void invalidate(int oid) {
        valid[oid] = false;
        counts[oid] = 0;
#ifdef __USEBITOPER__
        memset(bits[oid], 0, sizeof(bool)*size());
#endif
    }//end

    bool isValid(int oid) {
        return valid[oid];
    }//end

    int size() {
        return mySize;
    }//end

    int getSizeGridX() {
        return sizeGridX;
    }//end

    int getSizeGridY() {
        return sizeGridY;
    }//end

    int gridX(double x) {
        if (x < minPoint.x) return 0;
        if (x > maxPoint.x) return sizeGridX - 1;
        double result = floor(double((x - minPoint.x) / minDist));

        return result;
    }//end

    int gridX(Point pnt) {
        return gridX(pnt.x);
    }//end

    int gridY(double y) {
        if (y < minPoint.y) return 0;
        if (y > maxPoint.y) return sizeGridY - 1;
        return int(floor((y - minPoint.y) / minDist));

    }//end

    int gridY(Point pnt) {
        return gridY(pnt.y);
    }//end

    long double getAverageGlobalCountGrid() {
        unsigned long count = 0;
        unsigned long sum = 0;
        unsigned int cells = 0;
        for (int x = 0; x < this->getSizeGridX(); ++x) {
            for (int y = 0; y < this->getSizeGridY(); ++y) {
                count = this->getCountGrid(x,y);
                if (count != 0) {
                    sum += count;
                    ++cells;
                }
            }
        }
        return (long double) sum / (double) cells;
    }

    unsigned long getCountGrid(int x, int y) {
        return points[x][y].size();
    }//end

    unsigned long getCountGrid(Point pnt) {
        return getCountGrid(gridX(pnt), gridY(pnt));
    }//end

    int getCountGridExpanded(const unsigned int  x, const unsigned int  y) {
        int lower_x, upper_x, lower_y, upper_y;
        int total = 0;

        lower_x = upper_x = x;
        lower_y = upper_y = y;
        if (x > 0) lower_x--;
        if (x < sizeGridX - 1) upper_x++;
        if (y > 0) lower_y--;
        if (y < sizeGridY - 1) upper_y++;

        for (int i = lower_x; i <= upper_x; i++) {
            for (int j = lower_y; j <= upper_y; j++) {
                total += points[i][j].size();
            }//end for
        }//end for

        return total;
    }//end

    int getCountGridExpanded(Point pnt) {
        return getCountGridExpanded(gridX(pnt), gridY(pnt));
    }//end

    set<Point, less<Point> > getPointsGrid(int x, int y) {
        return points[x][y];
    }//end

    set<Point, less<Point> > getPointsGrid(Point pnt) {
        return getPointsGrid(gridX(pnt), gridY(pnt));
    }//end

    /**
     * Get a set of points
     */
    set<Point, less<Point> > getPointsExpanded(const unsigned int x, const unsigned int y) {
        set<Point, less<Point> >::iterator it;
        set<Point, less<Point> > pnts;
        int lower_x, upper_x, lower_y, upper_y;

        lower_x = upper_x = x;
        lower_y = upper_y = y;
        if (x > 0) lower_x--;
        if (x < sizeGridX - 1) upper_x++;
        if (y > 0) lower_y--;
        if (y < sizeGridY - 1) upper_y++;
        pnts.clear();

        for (int i = lower_x; i <= upper_x; i++) {
            for (int j = lower_y; j <= upper_y; j++) {
                for (it = points[i][j].begin(); it != points[i][j].end(); ++it) {
                    if (valid[(*it).oid] && (counts[(*it).oid] >= nroFlocks))
                        pnts.insert(*it);
                }//end for
            }//end for
        }//end for

        if (pnts.size() < nroFlocks) pnts.clear();

        return pnts;
    }//end

    set<Point, less<Point> > getPointsExpanded(Point pnt) {
        return getPointsExpanded(gridX(pnt), gridY(pnt));
    }//end

    set<Point, less<Point> >::iterator getIteratorBegin(int x, int y) {
        return points[x][y].begin();
    }//end

    set<Point, less<Point> >::iterator getIteratorBegin(Point pnt) {
        return getIteratorBegin(gridX(pnt), gridY(pnt));
    }//end

    set<Point, less<Point> >::iterator getIteratorEnd(int x, int y) {
        return points[x][y].end();
    }//end

    set<Point, less<Point> >::iterator getIteratorEnd(Point pnt) {
        return getIteratorEnd(gridX(pnt), gridY(pnt));
    }//end

private:
    set<Point, less<Point> > **points = nullptr;
    Point minPoint, maxPoint;

    bool **bits = nullptr;
    bool *valid = nullptr;
    double minDist = 0;
    double nroFlocks = 0;
    int *counts = nullptr;
    unsigned int mySize = 0;
    unsigned int sizeGridX = 0;
    unsigned int sizeGridY = 0;
};

#endif //__GRIDINDEX__
