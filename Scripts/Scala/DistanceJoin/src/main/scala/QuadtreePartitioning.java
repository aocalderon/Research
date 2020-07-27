/*
 * FILE: QuadtreePartitioning
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
package edu.ucr.dblab.djoin;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import java.io.Serializable;
import java.util.List;

public class QuadtreePartitioning implements Serializable
{
    /**
     * The Quad-Tree.
     */
    private final StandardQuadTree<Point> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary the boundary
     * @param partitions the partitions
     */
    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, int partitions, double epsilon)
            throws Exception
    {
        this(samples, boundary, partitions, -1, epsilon);
    }

    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, final int partitions, int minTreeLevel, double epsilon)
            throws Exception
    {
        // Make sure the tree doesn't get too deep in case of data skew
	GeometryFactory geofactory = new GeometryFactory(new PrecisionModel(1000));
        int maxLevel = partitions;
        int maxItemsPerNode = samples.size() / partitions;
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary), 0,
					     maxItemsPerNode, maxLevel, epsilon);
        if (minTreeLevel > 0) {
            partitionTree.forceGrowUp(minTreeLevel);
        }

        for (final Envelope sample : samples) {
	    Point geom = geofactory.createPoint(sample.centre());
            partitionTree.insert(new QuadRectangle(sample), geom);
        }

        partitionTree.assignPartitionIds();
    }

    public StandardQuadTree<Point> getPartitionTree()
    {
        return this.partitionTree;
    }
}
