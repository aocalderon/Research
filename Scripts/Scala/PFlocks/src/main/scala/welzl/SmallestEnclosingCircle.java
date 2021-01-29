/* 
 * Smallest enclosing circle - Library (Java)
 * 
 * Copyright (c) 2020 Project Nayuki
 * https://www.nayuki.io/page/smallest-enclosing-circle
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program (see COPYING.txt and COPYING.LESSER.txt).
 * If not, see <http://www.gnu.org/licenses/>.
 */

package edu.ucr.dblab.pflock.welzl.sec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.lang.Math; 

public final class SmallestEnclosingCircle {
    /* 
     * Returns the smallest circle that encloses all the given points. Runs in expected O(n) time, randomized.
     * Note: If 0 points are given, null is returned. If 1 point is given, a circle of radius 0 is returned.
     */
    
    // Initially: No boundary points known
    public static Circle linear(List<Point> points) {
	// Clone list to preserve the caller's data, randomize order
	List<Point> shuffled = new ArrayList<>(points);
	Collections.shuffle(shuffled, new Random());
		
	// Progressively add points to circle or recompute circle
	Circle c = null;
	for (int i = 0; i < shuffled.size(); i++) {
	    Point p = shuffled.get(i);
	    if (c == null || !c.contains(p))
		c = makeCircleOnePoint(shuffled.subList(0, i + 1), p);
	}
	return c;
    }
	
    // One boundary point known
    private static Circle makeCircleOnePoint(List<Point> points, Point p) {
	Circle c = new Circle(p, 0);
	for (int i = 0; i < points.size(); i++) {
	    Point q = points.get(i);
	    if (!c.contains(q)) {
		if (c.r == 0)
		    c = makeDiameter(p, q);
		else
		    c = makeCircleTwoPoints(points.subList(0, i + 1), p, q);
	    }
	}
	return c;
    }
	
    // Two boundary points known
    private static Circle makeCircleTwoPoints(List<Point> points, Point p, Point q) {
	Circle circ = makeDiameter(p, q);
	Circle left  = null;
	Circle right = null;
		
	// For each point not in the two-point circle
	Point pq = q.subtract(p);
	for (Point r : points) {
	    if (circ.contains(r))
		continue;
			
	    // Form a circumcircle and classify it on left or right side
	    double cross = pq.cross(r.subtract(p));
	    Circle c = makeCircumcircle(p, q, r);
	    if (c == null)
		continue;
	    else if (cross > 0 && (left == null || pq.cross(c.c.subtract(p)) > pq.cross(left.c.subtract(p))))
		left = c;
	    else if (cross < 0 && (right == null || pq.cross(c.c.subtract(p)) < pq.cross(right.c.subtract(p))))
		right = c;
	}
		
	// Select which circle to return
	if (left == null && right == null)
	    return circ;
	else if (left == null)
	    return right;
	else if (right == null)
	    return left;
	else
	    return left.r <= right.r ? left : right;
    }
	
    static Circle makeDiameter(Point a, Point b) {
	Point c = new Point((a.x + b.x) / 2, (a.y + b.y) / 2);
	return new Circle(c, Math.max(c.distance(a), c.distance(b)));
    }
	
    static Circle makeCircumcircle(Point a, Point b, Point c) {
	// Mathematical algorithm from Wikipedia: Circumscribed circle
	double ox = (Math.min(Math.min(a.x, b.x), c.x) + Math.max(Math.max(a.x, b.x), c.x)) / 2;
	double oy = (Math.min(Math.min(a.y, b.y), c.y) + Math.max(Math.max(a.y, b.y), c.y)) / 2;
	double ax = a.x - ox,  ay = a.y - oy;
	double bx = b.x - ox,  by = b.y - oy;
	double cx = c.x - ox,  cy = c.y - oy;
	double d = (ax * (by - cy) + bx * (cy - ay) + cx * (ay - by)) * 2;
	if (d == 0)
	    return null;
	double x = ((ax*ax + ay*ay) * (by - cy) + (bx*bx + by*by) * (cy - ay) + (cx*cx + cy*cy) * (ay - by)) / d;
	double y = ((ax*ax + ay*ay) * (cx - bx) + (bx*bx + by*by) * (ax - cx) + (cx*cx + cy*cy) * (bx - ax)) / d;
	Point p = new Point(ox + x, oy + y);
	double r = Math.max(Math.max(p.distance(a), p.distance(b)), p.distance(c));
	return new Circle(p, r);
    }
	
    /****************************/
    /*---- Helper functions ----*/
    /****************************/
	
    public static List<Point> makeRandomPoints(int n) {
	List<Point> result = new ArrayList<>();
	if (rand.nextDouble() < 0.2) {  // Discrete lattice (to have a chance of duplicated points)
	    for (int i = 0; i < n; i++)
		result.add(new Point(rand.nextInt(10), rand.nextInt(10)));
	} else {  // Gaussian distribution
	    for (int i = 0; i < n; i++)
		result.add(new Point(rand.nextGaussian(), rand.nextGaussian()));
	}
	return result;
    }
	
	
    // Returns the smallest enclosing circle in O(n^4) time using the naive algorithm.
    public static Circle naive(List<Point> points) {
	// Degenerate cases
	if (points.size() == 0)
	    return null;
	else if (points.size() == 1)
	    return new Circle(points.get(0), 0);
		
	// Try all unique pairs
	Circle result = null;
	for (int i = 0; i < points.size(); i++) {
	    for (int j = i + 1; j < points.size(); j++) {
		Circle c = SmallestEnclosingCircle.makeDiameter(points.get(i), points.get(j));
		if ((result == null || c.r < result.r) && c.contains(points))
		    result = c;
	    }
	}
	if (result != null)
	    return result;  // This optimization is not mathematically proven
		
	// Try all unique triples
	for (int i = 0; i < points.size(); i++) {
	    for (int j = i + 1; j < points.size(); j++) {
		for (int k = j + 1; k < points.size(); k++) {
		    Circle c = SmallestEnclosingCircle.makeCircumcircle(points.get(i), points.get(j), points.get(k));
		    if (c != null && (result == null || c.r < result.r) && c.contains(points))
			result = c;
		}
	    }
	}
	if (result == null)
	    throw new AssertionError();
	return result;
    }
	
	
    private static final double EPSILON = 1e-14;
	
    private static final Random rand = new Random();	
	
    public static void main(String[] args) {
	final int TRIALS = 1000;
	int pass = 0;
	int fail = 0;
	for (int i = 0; i < TRIALS; i++) {
	    List<Point> points = makeRandomPoints(rand.nextInt(30) + 1);
	    Circle reference = SmallestEnclosingCircle.naive(points);
	    Circle actual    = SmallestEnclosingCircle.linear(points);

	    if(Math.abs(reference.r - actual.r) > EPSILON){
		System.out.println("Radius error");
		fail++;
	    } else if(Math.abs(reference.c.x - actual.c.x) > EPSILON){
		System.out.println("X error");
		fail++;
	    } else if(Math.abs(reference.c.y - actual.c.y) > EPSILON){
		System.out.println("Y error");
		fail++;
	    } else {
		pass++;
	    }
	}
	System.out.println(TRIALS + " tests. Pass: " + pass + " Fail: " + fail);
    }	
}

