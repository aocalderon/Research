/*
 * (C) Copyright 2017-2020, by Dimitrios Michail and Contributors.
 *
 * JGraphT : a free Java graph-theory library
 *
 * See the CONTRIBUTORS.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the
 * GNU Lesser General Public License v2.1 or later
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR LGPL-2.1-or-later
 */

package edu.ucr.dblab;

import org.jgrapht.*;
import org.jgrapht.alg.clique.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Bron-Kerbosch maximal clique enumeration algorithm with pivot.
 * 
 * <p>
 * The pivoting follows the rule from the paper
 * <ul>
 * <li>E. Tomita, A. Tanaka, and H. Takahashi. The worst-case time complexity for generating all
 * maximal cliques and computational experiments. Theor. Comput. Sci. 363(1):28–42, 2006.</li>
 * </ul>
 * 
 * <p>
 * where the authors show that using that rule guarantees that the Bron-Kerbosch algorithm has
 * worst-case running time $O(3^{n/3})$ where $n$ is the number of vertices of the graph, excluding
 * time to write the output, which is worst-case optimal.
 * 
 * <p>
 * The algorithm first computes all maximal cliques and then returns the result to the user. A
 * timeout can be set using the constructor parameters.
 * 
 * @param <V> the graph vertex type
 * @param <E> the graph edge type
 * 
 * @see BronKerboschCliqueFinder
 * @see DegeneracyBronKerboschCliqueFinder
 *
 * @author Dimitrios Michail
 */

import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

class P{
    double x;
    double y;
    int id;
    Point point;
    
    public P(int id, double x, double y){
	PrecisionModel model = new PrecisionModel(1000);
	GeometryFactory geofactory = new GeometryFactory(model);
	this.point = geofactory.createPoint(new Coordinate(x, y));
	this.id = id;
	this.x = x;
	this.y = y;
    }
    public String wkt(){
	String wkt = point.toText();
	return wkt + "\t" + id;
    }
    public String toString(){
	return "" + id;
    }
}

class Clique<V>{
    Geometry geom;
    ArrayList<Integer> ids;
    
    public Clique(Set<V> points){
	PrecisionModel model = new PrecisionModel(1000);
	GeometryFactory geofactory = new GeometryFactory(model);
	int n = points.size();
	Coordinate[] coords = new Coordinate[n];
	ArrayList ids = new ArrayList<Integer>();
	int i = 0;
	for(V v: points){
	    P p = (P) v;
	    coords[i] = new Coordinate(p.x,p.y);
	    ids.add(p.id);
	    i = i + 1;
	}
	this.geom = geofactory.createMultiPoint(coords).convexHull();
	this.ids = ids;
    }
    public String wkt(){
	String wkt = geom.toText();
	StringBuffer sb = new StringBuffer();
      
	for (Integer id : ids) {
	    sb.append(id.toString());
	    sb.append(" ");
	}
	
	return wkt + sb.toString();
    }
}

public class PBKCliqueFinderTest<V, E>
    extends
    PivotBronKerboschCliqueFinder<V, E>
{

    StringBuffer logger = new StringBuffer();
    
    /**
     * Constructs a new clique finder.
     *
     * @param graph the input graph; must be simple
     */
    public PBKCliqueFinderTest(Graph<V, E> graph)
    {
        this(graph, 0L, TimeUnit.SECONDS);
    }

    /**
     * Constructs a new clique finder.
     *
     * @param graph the input graph; must be simple
     * @param timeout the maximum time to wait, if zero no timeout
     * @param unit the time unit of the timeout argument
     */
    public PBKCliqueFinderTest(Graph<V, E> graph, long timeout, TimeUnit unit)
    {
        super(graph, timeout, unit);
    }

    /**
     * Lazily execute the enumeration algorithm.
     */
    @Override
    protected void lazyRun()
    {
        if (allMaximalCliques == null) {
            if (!GraphTests.isSimple(graph)) {
                throw new IllegalArgumentException("Graph must be simple");
            }
            allMaximalCliques = new ArrayList<>();

            long nanosTimeLimit;
            try {
                nanosTimeLimit = Math.addExact(System.nanoTime(), nanos);
            } catch (ArithmeticException ignore) {
                nanosTimeLimit = Long.MAX_VALUE;
            }

            findCliques(
			new HashSet<>(graph.vertexSet()), new HashSet<>(), new HashSet<>(), nanosTimeLimit, 0);
        }
    }

    /**
     * Choose a pivot.
     * 
     * @param P vertices to consider adding to the clique
     * @param X vertices which must be excluded from the clique
     * @return a pivot
     */
    private V choosePivot(Set<V> P, Set<V> X)
    {
        int max = -1;
        V pivot = null;

        Iterator<V> it = Stream.concat(P.stream(), X.stream()).iterator();
        while (it.hasNext()) {
            V u = it.next();
            int count = 0;
            for (E e : graph.edgesOf(u)) {
                if (P.contains(Graphs.getOppositeVertex(graph, e, u))) {
                    count++;
                }
            }
            if (count > max) {
                max = count;
                pivot = u;
            }
        }

        return pivot;
    }

    /**
     * Recursive implementation of the Bron-Kerbosch with pivot.
     * 
     * @param P vertices to consider adding to the clique
     * @param R a possibly non-maximal clique
     * @param X vertices which must be excluded from the clique
     * @param nanosTimeLimit time limit
     */
    protected void findCliques(Set<V> P, Set<V> R, Set<V> X, final long nanosTimeLimit, final int iter)
    {
	print(iter, 0, "P", mkString(P, " "));
	print(iter, 0, "R", mkString(R, " "));
	print(iter, 0, "X", mkString(X, " "));

        /*
         * Check if maximal clique
         */
        if (P.isEmpty() && X.isEmpty()) {
	    Clique c = new Clique(R);
	    print(iter, 8, "Clique", mkString(R, " "));
	    
            Set<V> maximalClique = new HashSet<>(R);
            allMaximalCliques.add(maximalClique);
            maxSize = Math.max(maxSize, maximalClique.size());
            return;
        }

        /*
         * Check if timeout
         */
        if (nanosTimeLimit - System.nanoTime() < 0) {
            timeLimitReached = true;
            return;
        }

        /*
         * Choose pivot
         */	
        V u = choosePivot(P, X);
	print(iter, 1, "Pivot", u.toString());

        /*
         * Find candidates for addition
         */
        Set<V> uNeighbors = new HashSet<>();
        for (E e : graph.edgesOf(u)) {
            uNeighbors.add(Graphs.getOppositeVertex(graph, e, u));
        }
	print(iter, 2, "N_pivot", mkString(uNeighbors, " "));
	
        Set<V> candidates = new HashSet<>();
        for (V v : P) {
            if (!uNeighbors.contains(v)) {
                candidates.add(v);
            }
        }

	print(iter, 3, "Candidates", mkString(candidates, " "));

        /*
         * Main loop
         */
        for (V v : candidates) {
            Set<V> vNeighbors = new HashSet<>();
            for (E e : graph.edgesOf(v)) {
		V q = Graphs.getOppositeVertex(graph, e, v);
	        vNeighbors.add(q);
            }
	    
	    print(iter, 4, "v", v.toString());
	    print(iter, 4, "N_v", mkString(vNeighbors, " "));

            Set<V> newP = P.stream().filter(vNeighbors::contains)
		.collect(Collectors.toSet());
            Set<V> newX = X.stream().filter(vNeighbors::contains)
		.collect(Collectors.toSet());
            Set<V> newR = new HashSet<>(R);
	    // HERE IS WHEN THE POSSIBLE CLIQUE GROWTH...
	    // I SHOULD VERIFY IF THE NEW VERTEX IS STILL IN A mbc WITH EPSILON DIAMETER...
            newR.add(v);

            findCliques(newP, newR, newX, nanosTimeLimit, iter + 1);

	    print(iter, 6, "back", "{}");

	    P.remove(v);
            X.add(v);

	    print(iter, 7, "P", mkString(P, " "));
	    print(iter, 7, "R", mkString(R, " "));
	    print(iter, 7, "X", mkString(X, " "));
	    
        }
    }

    public String mkString(Set<V> A, String sep){
	StringBuffer sb = new StringBuffer();

	if(A.isEmpty()) return "{}";
	for (V v : A) {
	    sb.append(v);
	    sb.append(sep);
	}
	
	return sb.toString();
    }

    public void print(int iter, int step, String tag, String value){
	String log = iter + "|" + step + "|" + tag + "|" + value + "\n";
	logger.append(log);
	//System.out.print(log);
    }

    public String getLogger(){
	return logger.toString();
    }
}
