/*
Flock Evaluation Algorithms

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

#ifndef FLOCKS_HASHABLE_HPP
#define FLOCKS_HASHABLE_HPP

#include <bitset>
#include <hash/Hash.h>
#include <hash/MurMurHash.h>
#include <hash/SpookHash.h>


using namespace std;
using namespace flockcommon;

class Hashable {

public:

    Hashable() {createHashes();}

    Hashable(const vector<Hash *> &functions) : functions(functions) { createHashes(); }

    virtual void insert(const Point point) = 0;


    const bitset<SIGN_SIZE> &getSignature() const {
        return signature;
    }

protected:
    void hashPoint(const Point point) {
        for (auto function = functions.begin(); function != functions.end(); ++function) {

            u_int32_t position = (*function)->pureHash(&point.oid, sizeof(point.oid), 0);
            position = position % SIGN_SIZE;

            #ifdef FHASH_DEBUG
                cout << "Hash: " << (*function)->getName() << "(" << point.oid << ")\tValue: " << position << endl;
            #endif

            this->signature.set(position);
        }
    };

    void createHashes() {

        this->functions.push_back(new MurMurHash());
        this->functions.push_back(new SpookHash());

    }

    bitset<SIGN_SIZE> signature;
    vector<Hash *> functions;

    void clear() {
        this->signature.reset();
        if (functions.size() != 0) {
            for (auto func = functions.begin(); func != functions.end(); ++func) {
                if (*func != nullptr) delete *func;
            }
        }
        createHashes();
    }

};


#endif //FLOCKS_HASHABLE_HPP
