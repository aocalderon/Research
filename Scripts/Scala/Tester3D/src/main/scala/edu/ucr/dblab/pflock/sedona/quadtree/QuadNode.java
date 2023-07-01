/*
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 /***
 From Project Sedona [https://sedona.apache.org/1.4.1/].
 https://github.com/apache/sedona
 original package: org.apache.sedona.core.spatialPartitioning
 ***/

package edu.ucr.dblab.pflock.sedona.quadtree;

import java.io.Serializable;

public class QuadNode<T> implements Serializable
{
    QuadRectangle r;
    T element;
    int partitionId = -1;

    QuadNode(QuadRectangle r, T element)
    {
        this.r = r;
        this.element = element;
    }

    public QuadRectangle getQuadRectangle(){
	return r;
    }

    public T getElement(){
	return element;
    }

    public void setPartitionId(int id){
	this.partitionId = id;
    }

    public int getPartitionID(){
	return partitionId;
    }

    @Override
    public String toString()
    {
        return r.toString() + " " + element.toString();
    }
}
