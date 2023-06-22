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

public class PBKCliqueFinder<V, E> extends PivotBronKerboschCliqueFinder<V, E> {

    public PBKCliqueFinder(Graph<V, E> graph) {
        super(graph, 0L, TimeUnit.SECONDS);
    }

    @Override
    protected void lazyRun() {
        if (allMaximalCliques == null) {
            if (!GraphTests.isSimple(graph)) {
                throw new IllegalArgumentException("Graph must be simple");
            }
            allMaximalCliques = new ArrayList<>();
            findCliques(new HashSet<>(graph.vertexSet()),
			new HashSet<>(),
			new HashSet<>());
        }
    }

    private V choosePivot(Set<V> P, Set<V> X) {
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

    protected void findCliques(Set<V> P, Set<V> R, Set<V> X) {
        // Check if maximal clique
        if (P.isEmpty() && X.isEmpty()) {
            Set<V> maximalClique = new HashSet<>(R);
            allMaximalCliques.add(maximalClique);
            maxSize = Math.max(maxSize, maximalClique.size());
            return;
        }

	// Choose pivot
        V u = choosePivot(P, X);

	// Find candidates for addition
        Set<V> uNeighbors = new HashSet<>();
        for (E e : graph.edgesOf(u)) {
            uNeighbors.add(Graphs.getOppositeVertex(graph, e, u));
        }
        Set<V> candidates = new HashSet<>();
        for (V v : P) {
            if (!uNeighbors.contains(v)) {
                candidates.add(v);
            }
        }

        // Main loop
        for (V v : candidates) {
            Set<V> vNeighbors = new HashSet<>();
            for (E e : graph.edgesOf(v)) {
		V q = Graphs.getOppositeVertex(graph, e, v);
	        vNeighbors.add(q);
            }

            Set<V> newP = P.stream().filter(vNeighbors::contains)
		.collect(Collectors.toSet());
            Set<V> newX = X.stream().filter(vNeighbors::contains)
		.collect(Collectors.toSet());
	    
            Set<V> newR = new HashSet<>(R);
            newR.add(v);

            findCliques(newP, newR, newX);

	    P.remove(v);
            X.add(v);
        }
    }
}
