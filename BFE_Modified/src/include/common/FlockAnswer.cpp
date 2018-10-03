/*
Flock Evaluation Algorithms

Copyright (C) 2009 Marcos R. Vieira
Copyright (C) 2015 Pedro S. Tanaka
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

#include <vector>
#include <psb/HashPlaneBox.h>
#include "FlockAnswer.h"


using namespace std;

namespace flockcommon {

//==============================================================================
    unsigned int FlockAnswer::findAnswer() {

        if ((endTime - startTime) != (lengthFlocks - 1)) {
            endTime++;   // has to increment the endTime always!
            return 0; // no answer qualifies yet!
        }

        Cluster flock;
        vector<Cluster>::iterator itFlks;
        unsigned int nroAnswer = 0;

        for (itFlks = this->myAnswers.begin(); itFlks != this->myAnswers.end(); ++itFlks) {
            flock = (*itFlks);

            if ((flock.endTime - flock.startTime) == (lengthFlocks - 1)) {
                #ifdef __PRINTANSWER__
                    printAnswer(flock);
                #endif
                (*itFlks).startTime++;  // shift the time. BE CAREFULL THAT HAS TO BE USING THE ITERATOR: *itFlks
                nroAnswer++;  // found a flock! Report it!
            }//end if
        }//end for

        // remove duplicates. This can happens because there are flocks starting with different startTime time
        // and after updating the startTime attribute, it can have duplicates.
        if (nroAnswer > 0) checkDuplicateAnswer();

        startTime++;
        endTime++;   // has to increment the endTime always!

        return nroAnswer;
    }//end

//------------------------------------------------------------------------------
    void FlockAnswer::checkDuplicateAnswer() {

        if (this->myAnswers.empty()) return;

        vector<Cluster> newMyAnswers;
        vector<Cluster>::iterator itFlks1, itFlks2;
        unsigned int interCount;
        bool insert;
        Cluster flock;

        newMyAnswers.clear();
        // look for supersets of answers in MyAnswers
        for (itFlks1 = this->myAnswers.begin(); itFlks1 != this->myAnswers.end(); ++itFlks1) {

            insert = true;
            flock = *itFlks1;

            for (itFlks2 = newMyAnswers.begin(); insert && itFlks2 != newMyAnswers.end();) {
                if ((*itFlks1).startTime == (*itFlks2).startTime && (*itFlks1).endTime == (*itFlks2).endTime) {
                    // compute the intersection.
                    interCount = Cluster::countIntersection((*itFlks1).points, (*itFlks2).points);
                    if ((*itFlks1).points.size() == interCount) {
                        insert = false; // subset: no need to insert it.
                    } else if ((*itFlks2).points.size() == interCount) {
                        newMyAnswers.erase(itFlks2);  // superset: remove the old flock.
                    } else {
                        ++itFlks2;
                    }//end if
                } else {
                    ++itFlks2;
                }//end if
            }//end for

            if (insert) {
                newMyAnswers.insert(newMyAnswers.end(), flock);
            }//end if
        }//end for

        this->myAnswers = newMyAnswers; // make it new

        if (this->isIndexed()) {
            clock_t pTimeBuildIdx = clock();
            this->myIndex.reset();
            this->myIndex.insertDocuments(this->myAnswers);
            this->timeBuildIdx += (clock() - pTimeBuildIdx) / CLOCK_DIV;
        }
    }//end

//------------------------------------------------------------------------------
    void FlockAnswer::printAnswer(Cluster flock) {
        //cout << endl << " ANSWER size: "
        //     << flock.points.size()
        //     << " (" << flock.startTime << ":" << flock.endTime << ") [";

        cout << endl << flock.startTime << ", " << flock.endTime << ", ";

        for (PointSet::my_const_iter it=flock.points.cbegin(); it!=flock.points.cend(); ++it){
            if (it!=flock.points.cbegin()) cout << " ";
            cout << (*it).oid;
        }
        //cout << "]";
    }

//------------------------------------------------------------------------------
    bool FlockAnswer::insertNewAnswer(Cluster flock) {

        if (flock.startTime < this->startTime || flock.endTime > this->endTime) {
            cerr << endl << "Error: Time range of the new flock does not match time range in the flock"
                 << " flock.StartTime:" << flock.startTime
                 << " startTime:" << this->startTime
                 << " flock.endTime:" << flock.endTime
                 << " endTime:" << this->endTime;

            return false;
        }//end if

        if (this->myAnswers.empty()) { // it's the first time
            this->myAnswers.insert(this->myAnswers.end(), flock); // no need to test anything.

            return true; // stop it.
        }//end if

        vector<Cluster>::iterator itFlks;
        unsigned int interCount;
        Cluster tmpFlock;

        // look for supersets of answers in MyAnswers
        for (itFlks = this->myAnswers.begin(); itFlks != this->myAnswers.end();) {
            tmpFlock = (*itFlks);
            interCount = Cluster::countIntersection(tmpFlock.points, flock.points);

            // let's try it!
            if (flock.points.size() == interCount) { // flock subset of tmpFlock
                return false; // don't need to insert it.
            }//end if
            if (tmpFlock.points.size() != interCount) {
                ++itFlks;  // keep it.
            } else {
                this->myAnswers.erase(itFlks);  // remove the answer. It's a subset of the new one.
            }//end if
        }//end for

        // we can safe insert the new answer.
        this->myAnswers.insert(this->myAnswers.end(), flock);

        return true; // inserted it.
    }//end

//------------------------------------------------------------------------------
    void FlockAnswer::mergeAnswer(vector<PointSet> newClusters, unsigned
    int time
    ) {
        vector<PointSet>::iterator itClts;
        vector<Cluster>::iterator itFlks;
        vector<Cluster> newFlocks;
        Cluster newFlk(time);
        Cluster oldFlk;
        PointSet newCluster;

        if (this->myAnswers.empty()) {  // this is the first time!
// it's the first time, just add all clusters in Flocks
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newFlk.points = *itClts;
                newFlk.points.invalidateCenter();

                insertNewAnswer(newFlk);
            }//end for
        } else {
// we have something for the previous time.
            newFlocks.clear();

// part to "merge" the new flocks with the old ones (time-1).
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newCluster = *itClts;

                for (itFlks = this->myAnswers.begin(); itFlks != this->myAnswers.end(); ++itFlks) {
                    oldFlk = (*itFlks);
                    newFlk.points = Cluster::setIntersection(oldFlk.points, newCluster);

                    if (newFlk.points.size() >= nroFlocks) {
// build the join result for time and flocks
                        newFlk.points.invalidateCenter();
                        newFlk.points.time = time;

                        newFlk.startTime = oldFlk.startTime;  // set the time first.
                        newFlk.endTime = time;
// newFlk is the result of join. All the results in newFlocks
// may have different startTime, but HAVE to have equal endTime.
                        Cluster::insertNewFlock(newFlocks, newFlk
                        );
                    }//end if
                }//end for
            }//end for
// part to insert the new flocks for the current "time".
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
// now we have to insert only the new clusters, where (endTime - startTime = 1)
                newFlk.points = *itClts;
                newFlk.points.invalidateCenter();
                newFlk.points.time = time;

                newFlk.startTime = time;  // set the time first.
                newFlk.endTime = time;
// insert the new cluster in newFlocks.
                Cluster::insertNewFlock(newFlocks, newFlk);
            }//end for

            this->myAnswers = newFlocks;  // make newFlocks Flocks
        }//end if
    }//end

    void FlockAnswer::mergeAnswer(vector<HashCluster> newClusters, unsigned int time) {
        vector<HashCluster>::iterator itClts;
        vector<Cluster>::iterator itFlks;
        Cluster newFlk(time);

        if (this->myAnswers.empty()) {  // this is the first time!
            // it's the first time, just add all clusters in Flocks
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newFlk.points.points = itClts->points;
                newFlk.points.invalidateCenter();

                newFlk.startTime = time; // set startTime time.
                newFlk.endTime = time + 1; // set end time.
                insertNewAnswer(newFlk);
            }//end for
        } else {
            Cluster oldFlk;
            PointSet newCluster;
            vector<Cluster> newFlocks;

            // part to "merge" the new flocks with the old ones (time-1).
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newCluster = *itClts;
                for (itFlks = this->myAnswers.begin(); itFlks != this->myAnswers.end(); ++itFlks) {
                    oldFlk = (*itFlks);
                    newFlk.points = Cluster::setIntersection(oldFlk.points, newCluster);

                    if (newFlk.points.size() >= nroFlocks) {
                        // build the join result for time and flocks
                        newFlk.points.invalidateCenter();
                        newFlk.points.time = time;

                        newFlk.startTime = oldFlk.startTime;  // set the time first.
                        newFlk.endTime = time;
                        // newFlk is the result of join. All the results in newFlocks
                        // may have different startTime, but HAVE to have equal endTime.
                        Cluster::insertNewFlock(newFlocks, newFlk);
                    }//end if
                }//end for
            }//end for
            // part to insert the new flocks for the current "time".
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                // now we have to insert only the new clusters, where (endTime - startTime = 1)
                newFlk.points = *itClts;
                newFlk.points.invalidateCenter();
                newFlk.points.time = time;

                newFlk.startTime = time;  // set the time first.
                newFlk.endTime = time;
                // insert the new cluster in newFlocks.
                Cluster::insertNewFlock(newFlocks, newFlk);
            }//end for
            this->myAnswers = newFlocks;  // make newFlocks Flocks
        }//end if
    }

    void FlockAnswer::mergeAnswerIndexed(vector<HashCluster> newClusters, unsigned int time) {

        vector<HashCluster>::iterator itClts;
        Cluster newFlk(time);

        if (this->myAnswers.empty()) {  // this is the first time!
            // it's the first time, just add all clusters in Flocks
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newFlk.points.points = itClts->points;
                newFlk.points.invalidateCenter();

                insertNewAnswer(newFlk);
            }//end for
        } else {
            Cluster oldFlk;
            PointSet newCluster;
            vector<Cluster> newFlocks;

            // part to "merge" the new flocks with the old ones (time-1).
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newCluster = *itClts;
                set<long> docIds = this->myIndex.countThresholdQuery(newCluster, nroFlocks);
                this->clustersPruned += ( this->myAnswers.size() - docIds.size() );

                for (auto docId = docIds.begin(); docId != docIds.end(); ++docId) {
                    oldFlk = this->myAnswers.at((unsigned long) *docId);
                    newFlk.points = Cluster::setIntersection(oldFlk.points, newCluster);

                    if (newFlk.points.size() >= nroFlocks) {
                        // build the join result for time and flocks
                        newFlk.points.invalidateCenter();
                        newFlk.points.time = time;

                        newFlk.startTime = oldFlk.startTime;  // set the time first.
                        newFlk.endTime = time;
                        // newFlk is the result of join. All the results in newFlocks
                        // may have different startTime, but HAVE to have equal endTime.
                        Cluster::insertNewFlock(newFlocks, newFlk);
                    }//end if
                }//end for
            }//end for
            // part to insert the new flocks for the current "time".
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                // now we have to insert only the new clusters, where (endTime - startTime = 1)
                newFlk.points = *itClts;
                newFlk.points.invalidateCenter();
                newFlk.points.time = time;

                newFlk.startTime = time;  // set the time first.
                newFlk.endTime = time;
                // insert the new cluster in newFlocks.
                Cluster::insertNewFlock(newFlocks, newFlk);
            }//end for

            this->myAnswers = newFlocks;  // make newFlocks Flocks

        }//end if

        //FIXME: dumb "strategy"
        this->myIndex.reset();
        clock_t p_timeIdx = clock();
        this->myIndex.insertDocuments(this->myAnswers);
        this->timeBuildIdx += (clock() - p_timeIdx) / CLOCK_DIV;
    }


    void FlockAnswer::mergeAnswerIndexed(vector<PointSet> newClusters, unsigned int time) {

        vector<PointSet>::iterator itClts;
        Cluster newFlk(time);

        if (this->myAnswers.empty()) {  // this is the first time!
            // it's the first time, just add all clusters in Flocks
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newFlk.points = *itClts;
                newFlk.points.invalidateCenter();

                insertNewAnswer(newFlk);
            }//end for
        } else {
            Cluster oldFlk;
            PointSet newCluster;
            vector<Cluster> newFlocks;

            // part to "merge" the new flocks with the old ones (time-1).
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                newCluster = *itClts;
                set<long> docIds = this->myIndex.countThresholdQuery(newCluster, nroFlocks);
//                if(docIds.size() != 0)  cout << "Query result set size: " << docIds.size() << endl;
                this->clustersPruned += ( this->myAnswers.size() - docIds.size() ) * newCluster.size();

                for (auto docId = docIds.begin(); docId != docIds.end(); ++docId) {
                    oldFlk = this->myAnswers.at((unsigned long) *docId);
                    newFlk.points = Cluster::setIntersection(oldFlk.points, newCluster);
                    if (newFlk.points.size() >= nroFlocks) {
                        // build the join result for time and flocks
                        newFlk.points.invalidateCenter();
                        newFlk.points.time = time;

                        newFlk.startTime = oldFlk.startTime;  // set the time first.
                        newFlk.endTime = time;
                        // newFlk is the result of join. All the results in newFlocks
                        // may have different startTime, but HAVE to have equal endTime.
                        Cluster::insertNewFlock(newFlocks, newFlk);
                    }//end if
                }//end for
            }//end for
            // part to insert the new flocks for the current "time".
            for (itClts = newClusters.begin(); itClts != newClusters.end(); ++itClts) {
                // now we have to insert only the new clusters, where (endTime - startTime = 1)
                newFlk = *itClts;
                newFlk.points.invalidateCenter();
                newFlk.points.time = time;

                newFlk.startTime = time;  // set the time first.
                newFlk.endTime = time;
                // insert the new cluster in newFlocks.
                Cluster::insertNewFlock(newFlocks, newFlk);
            }//end for

            this->myAnswers = newFlocks;  // make newFlocks Flocks

        }//end if

        //FIXME: dumb "strategy"
//        cout << "Index size: " << this->myIndex.size() << endl;
        this->myIndex.reset();
        clock_t p_timeIdx = clock();
        this->myIndex.insertDocuments(this->myAnswers);
        this->timeBuildIdx += (clock() - p_timeIdx) / CLOCK_DIV;
    }
}
