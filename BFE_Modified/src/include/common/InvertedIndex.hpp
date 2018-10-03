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

#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "Cluster.h"
#include "Point.h"

/**
 * The class is to be used to index clusters, but is generalized to index
 * any type via templates.
 *
 * The template parameters represents:
 *  * TKey: The type of the Key, where key is the search subject.
 *  * TDoc: The type of the document.
 *  * TDocId: the identifier of the document, an easy way to find the document.
 *  could be an index (int) or a iterator, choose as you see fit.
 *
 *  If one want to add support for some new type of document, first it is required
 *  to write a tokenize() method specialization for that type.
 *
 *  Sample usage:
 *
 * <code>
 * InvertedIndex<int,Cluster,long> myIdx;
 *
 * myIdx.insertDocument(doc, docId);
 *
 * // Gather clusters
 *
 * vector<long> result = myIdx.andQuery(clusters);
 * </code>
 *
 *
 * @author Pedro Tanaka
 **/
template<class TKey, class TDoc, class TDocId>
class InvertedIndex {

public:
    typedef typename std::map<TKey, ::std::set<TDocId>>::iterator idx_iterator;
    typedef typename std::vector<TDocId>::iterator bck_iterator;

    template <class TCol>
    bool insertDocument(TCol document, TDocId id) {
        std::vector<TKey> keys = tokenize(document);

        if (keys.empty() || keys.size() == 0) {
            return false;
        }

        for (auto key = keys.begin(); key != keys.end(); ++key) {
            this->index[*key].insert(id);
        }

        return true;
    }


    /**
     * Insert all the documents of the vector, and use the index on the vector as the
     * document identifier inside the index.
     * In this particular case the
     */
    bool insertDocuments(vector<TDoc> &documents) {
        bool status = true;
        for (unsigned int i = 0; i < documents.size(); ++i) {
            status = status && insertDocument(documents.at(i), i);
        }
        return status;
    }

    /**
 * This method only return the DocId's from the documents that have
 * all the keys inside <code>keys</code> (Intersection).
 */
    std::set<TDocId> andQuery(std::set<TKey> &keys) {

        auto keyIt = keys.begin();
        InvertedIndex::idx_iterator idxIt = this->index.find(*keyIt);
        std::set<TDocId> result = idxIt->second;
        std::set<TDocId> interRes;

        do {
            idxIt = this->index.find(*keyIt);
            interRes.clear();

            set_intersection(result.begin(), result.end(),
                             idxIt->second.begin(), idxIt->second.end(),
                             inserter(interRes, interRes.end()));

            // Accumulate the intersection on result
            result = interRes;

            // Move to the next key
            ++keyIt;
        } while (keyIt != keys.end());

        return result;
    }

    /**
     * Same as countThresholdQuery(vector, unsigned int), but in order to use this method one should
     * use int as the TDocId and TKey template parameters.
     *
     * @param PointSet& keys the point set that should be compared.
     */
    std::set<TDocId> countThresholdQuery(flockcommon::PointSet &keys, unsigned int threshold) {
        std::map<TDocId, unsigned int> intersectionCount;
        std::set<TDocId> docs;
        InvertedIndex::idx_iterator indexIt;

        for (auto key = keys.begin(); key != keys.end(); ++key) { // Iterate keys
            indexIt = this->index.find(key->oid); // Search it in the index

            if (indexIt == this->index.end()) continue; // Did not find, go to the next key

            // For each document that the key appeared,
            for (auto docsIt = indexIt->second.begin(); docsIt != indexIt->second.end(); ++docsIt) {
                intersectionCount[*docsIt]++; // Increment the intersection count
            }
        }

        for (auto docIt = intersectionCount.begin(); docIt != intersectionCount.end(); ++docIt) {
            if (docIt->second >= threshold) {
                docs.insert(docIt->first);
            }
        }

        return docs;
    }

    /**
     *
     */
    void reset() {
        index.clear(); // Just clear the index, this operation is clear-safe.
    }

/**
 * How many keys there are in the index.
 */
unsigned long size() {
    return this->index.size();
}

private:
    std::map<TKey, std::set<TDocId> > index;

    std::vector<TKey> tokenize(std::string fileName) {
        std::ifstream inputStream;
        std::string curLine;
        std::vector<std::string> tokens;
        std::string sub;


        inputStream.open(fileName.c_str());
        while (getline(inputStream, curLine)) {
            std::istringstream iss(curLine);

            do {
                iss >> sub;
                tokens.push_back(sub);
            } while (iss);

        }

    }

    std::vector<TKey> tokenize(flockcommon::Cluster cluster) {
        std::vector<TKey> tokens;

        for (auto point = cluster.points.begin(); point != cluster.points.end(); ++point) {
            tokens.push_back(point->oid);
        }

        return tokens;
    }

};

