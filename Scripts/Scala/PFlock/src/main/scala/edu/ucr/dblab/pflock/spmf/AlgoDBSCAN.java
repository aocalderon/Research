package edu.ucr.dblab.pflock.spmf;

/* This file is copyright (c) 2008-2013 Philippe Fournier-Viger
* 
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* 
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
/* This file is copyright (c) 2008-2015 Philippe Fournier-Viger
* 
* This file is part of the SPMF DATA MINING SOFTWARE
* (http://www.philippe-fournier-viger.com/spmf).
* 
* SPMF is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* SPMF. If not, see <http://www.gnu.org/licenses/>.
*/

/**
 * An implementation of the DBSCAN algorithm (Ester et al., 1996). 
 * Note that original algorithm suggested using a R*-tree to index points 
 * to avoid having a O(n^2) complexity, but we instead used a KD-Tree.
 * The DBScan algorithm was originally published in:
 * <br/><br/>
 * 
 * Ester, Martin; Kriegel, Hans-Peter; Sander, J�rg; Xu, Xiaowei (1996). Simoudis, Evangelos; 
 * Han, Jiawei; Fayyad, Usama M., eds. A density-based algorithm for discovering clusters in 
 * large spatial databases with noise. Proceedings of the Second International Conference on Knowledge
 *  Discovery and Data Mining (KDD-96). AAAI Press. pp. 226�231.
 * 
 * @author Philippe Fournier-Viger
 */

public class AlgoDBSCAN {
	protected List<Cluster> clusters = null;
	protected long startTimestamp; // the start time of the latest execution
	protected long endTimestamp;  // the end time of the latest execution
	long numberOfNoisePoints; // the number of iterations that was performed
	DistanceFunction distanceFunction = new DistanceEuclidian(); 
	KDTree kdtree;
	List<DoubleArray> bufferNeighboors1 = null;
	List<DoubleArray> bufferNeighboors2 = null;

	public AlgoDBSCAN() { 
	}
	
	public List<Cluster> run(List<DoubleArray> points, int minPts, double epsilon) throws NumberFormatException, IOException {
		startTimestamp =  System.currentTimeMillis();
		numberOfNoisePoints =0;
		kdtree = new KDTree();
		kdtree.buildtree(new ArrayList(points));
		clusters = new ArrayList<Cluster>();
		bufferNeighboors1 = new ArrayList<DoubleArray>();
		bufferNeighboors2 = new ArrayList<DoubleArray>();
		
		for(DoubleArray point : points) {
			if(point.visited == false) {
				point.visited = true;
				bufferNeighboors1.clear();
				kdtree.pointsWithinRadiusOf(point, epsilon, bufferNeighboors1);
				if(bufferNeighboors1.size() >= minPts -1) { // - 1 because we don't count the point itself in its neighborood
					expandCluster(point, bufferNeighboors1, epsilon, minPts);
				}
			}
		}
		for(DoubleArray point: points) {
			if(((DoubleArray)point).cluster == null){
				numberOfNoisePoints++;
			}
		}
		endTimestamp =  System.currentTimeMillis();
		bufferNeighboors1 = null;
		bufferNeighboors2 = null;
		kdtree = null;

		return clusters;
	}

	private void expandCluster(DoubleArray currentPoint,	List<DoubleArray> neighboors, double epsilon, int minPts) {	
		Cluster cluster = new Cluster();
		clusters.add(cluster);
		cluster.addVector(currentPoint);
		currentPoint.cluster = cluster;
		for(int i = 0; i < neighboors.size(); i++) {
			DoubleArray newPointDBS = (DoubleArray) neighboors.get(i);
			if(newPointDBS.visited == false) {
				newPointDBS.visited = true;
				bufferNeighboors2.clear();
				kdtree.pointsWithinRadiusOf(newPointDBS, epsilon, bufferNeighboors2);
				if(bufferNeighboors2.size() >= minPts - 1) { // - 1 because we don't count the point itself in its neighborood
					neighboors.addAll(bufferNeighboors2);
				}
			}
			if(newPointDBS.cluster == null){
				cluster.addVector(newPointDBS);
				newPointDBS.cluster = cluster;
			}
		}
	}

	/**
	 * Print statistics of the latest execution to System.out.
	 */
	public void printStatistics() {
		System.out.println("========== DBSCAN - SPMF 2.09 - STATS ============");
		System.out.println(" Total time ~: " + (endTimestamp - startTimestamp) + " ms");
		System.out.println(" SSE (Sum of Squared Errors) (lower is better) : " + ClustersEvaluation.getSSE(clusters, distanceFunction));
		System.out.println(" Number of noise points: " + numberOfNoisePoints);
		System.out.println(" Number of clusters: " + clusters.size());
		System.out.println("=====================================");
	}

}
