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

#include <BasicFlock.h>
#include <common/PointPair.h>

//==============================================================================
unsigned int BasicFlock::start(enum FLOCKQUERYMETHOD type) {

    if (this->openStream() == -1)
        exit(1); // problems opening the stream.

    unsigned int time = 0;
    clock_t partial_time, b_time;
    long double totalTime      = 0.0,
                timeIdx        = 0.0,
                partialTimeIdx = 0.0;
    unsigned long allClusters, filteredClusters, mergedClusters;
    long double avgCountGrid;
    this->totalCenters = 0;
    this->totalPairs = 0;
    this->totalClusters = 0;
    this->totalAnswers = 0;

    // Read the stream for one timestamp.
    // Current ts.
    long currTime = 0;

    while ((currTime = this->loadLocations()) >= 0) {
        mergedClusters = filteredClusters = allClusters = 0;
        avgCountGrid = 0.0;

        if (this->loggerIsGood()) {
            this->logMessage("\nMETRICS:::\tTS:" + to_string(currTime));
            this->logMessage("NUM_POINTS:" + to_string(this->myPoints.size()));
        }

#ifdef __DEBUGSTEPS__
        cout << endl << "t:" << time << " obj:" << this->myPoints.size() << flush;
#endif
        if (this->myPoints.size() >= this->nroFlocks) {
            // process the flocks for time.
            switch (type) {
                case BFE_METHOD: {
                    b_time = clock();
                    //cout << endl << " buiding the index..." << flush;
                    this->myGridIndex = new GridIndex(this->minX, this->maxX, this->minY, this->maxY,
                                                      this->maxOid + 1, this->myPoints.points, this->distanceFlocks,
                                                      this->nroFlocks);
                    partialTimeIdx = (clock() - b_time) / CLOCK_DIV;
                    timeIdx += partialTimeIdx;
                    totalTime += partialTimeIdx;

                    avgCountGrid = this->myGridIndex->getAverageGlobalCountGrid(); // CAUTION: very costly operation

                    partial_time = clock();
                    queryJoin(time, type, allClusters, filteredClusters, mergedClusters);

                    delete this->myGridIndex;
                    // Prevents double delete in destructor if used.
                    myGridIndex = nullptr;

                    break;
                }//end case

                case BFE_PSB_IVIDX_METHOD: { // PSI
                    clock_t p_timeJoin;

                    // Get clusters for this timestamp
                    this->mySweep = new PlaneSweep(this->distanceFlocks, this->nroFlocks, this->myPoints.points);

                    partial_time = clock();
                    vector<HashCluster> current = this->mySweep->getValidHashClusters();
                    this->timeSearch += (this->mySweep->getTimeSweepClustering() + this->mySweep->getTimeBoxes());
//                    cout << "Time box clustering: " << this->mySweep->getTimeBoxClustering() << endl;

                    this->totalPairs += this->mySweep->getPairs();
                    this->totalClusters += current.size();
                    this->totalCenters += this->mySweep->getCenters();


#ifdef PS_STEPS_DBG
                    cout << "Clusters TS#" << time << ": " << current.size() << endl;
#endif

                    // ST Merge
                    p_timeJoin = clock();
                    myFlockAnswers.mergeAnswerIndexed(current, time);
                    myFlockAnswers.setIndexed(true);
                    this->totalAnswers += myFlockAnswers.findAnswer();
                    this->timeJoin += (clock() - p_timeJoin) / CLOCK_DIV;

                    //METRICS
                    allClusters = this->mySweep->getClusterCount();
                    filteredClusters = current.size();
                    mergedClusters = myFlockAnswers.size();

                    delete this->mySweep;
                    // Prevents double delete in destructor if used.
                    mySweep = nullptr;

                    break;
                }; // end case BFE_PSB_IVIDX

                default:
                    cerr << endl << "Unknown method " << type << "!";
                    break;
            }//end - switch type

            totalTime += (clock() - partial_time) / CLOCK_DIV;
        } else {
            myFlockAnswers.shift(); // there is a gap in the answer. Clean it!
        }//end if/**/
        time++; // update time.

#ifdef __PRINTTIMESTATUS
        if (!(time % 1000)) cout << "." << flush;
#endif
#ifdef __MAXTIME__
        if (time > MAXTIME) break;
#endif

        if (this->loggerIsGood()) {
            this->logMessage("ALL_CLUSTERS:" + to_string(allClusters));
            this->logMessage("CLUSTERS_FILTERED:" + to_string(filteredClusters));
            this->logMessage("CLUSTERS_MERGED:" + to_string(mergedClusters));

            if (type == BFE_METHOD) {
                this->logMessage("AVG_GRID_COUNT:" + to_string(avgCountGrid));
            }
        }
    }//end while

    // clean points
    this->myPoints.clear();

    cout << endl << "totalTime(ms):\t" << totalTime
    << "\ttimeSearch:\t" << this->timeSearch
    << "\ttimeSetTest:\t" << this->timeSetTest
    << "\ttimeJoin:\t" << this->timeJoin
    << "\ttimeBuildIdx:\t" << timeIdx;

    cout << "\ttotalAnswers:\t" << this->totalAnswers << endl
    << "totalPairs:\t" << this->totalPairs
    << "\ttotalCenters:\t" << this->totalCenters
    << "\ttotalClusters:\t" << this->totalClusters;

    if (type == BFE_PSB_IVIDX_METHOD) {
        cout << "\ttimeInvIdx:\t" << this->myFlockAnswers.getTimeBuildIdx()
        << "\tclustersPruned:\t" << this->myFlockAnswers.getClustersPruned();
    }

    // Close the stream
    this->closeStream();

    return this->totalAnswers;
}//end

//------------------------------------------------------------------------------
/**
 * Join the discs from the timestamp represented by
 */
void BasicFlock::queryJoin(unsigned int time, FLOCKQUERYMETHOD type, unsigned long &allClusters,
                           unsigned long &filteredClusters,
                           unsigned long &mergedClusters) {
    PointPair pair;
    Center center;
    PointSet cluster;
    vector<PointSet> clusters;
    CenterPair centerPair;
    set<Point, less<Point> >::iterator it_g, it_e, it_f;
    set<Point, less<Point> > ePoints;
    set<Point, less<Point> > *neighborhoodPnts = nullptr;
    double dist, tmp;
    unsigned int pairs = 0;
    unsigned int centers = 0;
    unsigned int clusterCount = 0;

    clock_t p_timeSearch, p_timeSetTest, p_timeJoin;

    ////////////////////////////////////////
    // 1st step: find pair of points
    ////////////////////////////////////////
    for (int x = 0; x < this->myGridIndex->getSizeGridX(); x++) {
        for (int y = 0; y < this->myGridIndex->getSizeGridY(); y++) {
            if (this->myGridIndex->getCountGrid(x, y) > 0) { // Find grid cell that contains some points
                ePoints = this->myGridIndex->getPointsExpanded(x, y);

                // if |Ps| ≥ µ then
                if (ePoints.size() < this->nroFlocks)
                    continue;

                for (it_g = this->myGridIndex->getIteratorBegin(x, y);
                     it_g != this->myGridIndex->getIteratorEnd(x, y); ++it_g) { // For all points in the current cell

                    /*
                     * The result of such range search with more or equal points than µ (|H|
                     * ≥ µ) is stored in the list H that is used to check for each disk
                     * computed.
                     */
                    neighborhoodPnts = it_g->getNeighborhood(ePoints, this->distanceFlocks);

                    if (neighborhoodPnts->size() < this->nroFlocks) {
                        delete neighborhoodPnts;
                        continue;
                    }

                    for (it_e = neighborhoodPnts->begin();
                         it_e != neighborhoodPnts->end(); ++it_e) { // For all points in the cells
                        // of the neighborhood

                        if ((*it_g).oid < (*it_e).oid) {
                            if (this->myGridIndex->qualify(it_g, it_e)) {
                                // this pair qualifies. Add it into pairs.
                                pair[0] = *it_g;
                                pair[1] = *it_e;
                                pairs++;

                                ////////////////////////////////////////
                                // 2nd step: find the centers for each pair
                                ////////////////////////////////////////
                                centerPair = pair.calculateCenters(radius);

                                if (centerPair.valid) { // problems with inf and nan
                                    centers += 2;

                                    ////////////////////////////////////////
                                    // 3rd step: cluster points based on each center
                                    ////////////////////////////////////////
                                    for (int idx = 0; idx < 2; idx++) {
                                        p_timeSearch = clock();
                                        center = centerPair.get(idx); // get the center.
                                        cluster.clear();   // clean the cluster first.
                                        cluster.setCenter(center, radius);

                                        for (it_f = neighborhoodPnts->begin(); it_f !=
                                                neighborhoodPnts->end(); ++it_f) {
                                            // compute the distance.
                                            tmp = (*it_f).x - center.x;
                                            dist = tmp * tmp;
                                            tmp = (*it_f).y - center.y;
                                            dist += tmp * tmp;
                                            dist = sqrt(dist);

                                            if (DOUBLE_LEQ(dist, radius)) {
                                                // insert point into cluster
                                                cluster.insert(*it_f);
                                            }//end if
                                        }//end for

                                        this->timeSearch += (clock() - p_timeSearch) / CLOCK_DIV;

                                        // does this cluster qualify?
                                        if (cluster.size() >= this->nroFlocks) {
                                            // insert if there is no duplicates in clusters.
                                            p_timeSetTest = clock();
                                            ++clusterCount;
                                            Cluster::insertCluster(clusters, cluster);
                                            timeSetTest += (clock() - p_timeSetTest) / CLOCK_DIV;
                                        }//end if
                                    }//end for
                                }//end if
                            }//end if
                        }//end if
                    }//end for

                    delete neighborhoodPnts;
                }//end for
                ePoints.clear();
            }//end if
        }//end for
    }//end for

#ifdef __DEBUGSTEPS__
    cout << " pairs:" << pairs
         << " centers:" << centers
         << " clusters:" << clusters.size() << flush;
#endif

    this->totalPairs += pairs;
    this->totalCenters += centers;
    this->totalClusters += clusters.size();
#ifdef __PRINTCLUSTERS__
    if (time >= 12 && time <= 25) {
        cout << "\n#Clusters: " << clusters.size() << "; Time: " << time;

        cout << "\n## BEGIN CLUSTERS @" << time << "\n";
        for (auto clus = clusters.begin(); clus != clusters.end(); ++clus) {
            cout << clus->toString() << "\n";
        }
        cout << "## BEGIN CLUSTERS @" << time << endl;
    }
#endif


    ////////////////////////////////////////
    // 4th step: merge clusters(time) with clusters(time-1)
    ////////////////////////////////////////
    p_timeJoin = clock();

    myFlockAnswers.mergeAnswer(clusters, time);

    allClusters = clusterCount;
    filteredClusters = cluster.size();
    mergedClusters = myFlockAnswers.size();
    this->totalAnswers += myFlockAnswers.findAnswer();
    this->timeJoin += (clock() - p_timeJoin) / CLOCK_DIV;
}//end
