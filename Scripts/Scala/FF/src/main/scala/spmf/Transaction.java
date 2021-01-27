package edu.ucr.dblab.pflock.spmf;

/* This file is copyright (c) 2012-2014 Alan Souza
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

import java.util.Arrays;
import java.util.List;
import com.vividsolutions.jts.geom.Point;

public class Transaction implements Comparable<Transaction>{
    private static Integer[] temp = new Integer[500];
    private Transaction originalTransaction;
    private int offset;
    private Integer[] items;
    private Double x = 0.0;
    private Double y = 0.0;
    private Point center = null;
    private Integer s = -1;
    private Integer e = -1;

    Transaction(Integer[] items) {
    	originalTransaction = this;
    	this.items = items;
    	this.offset = 0;
    }
    
    Transaction(Transaction transaction, int offset) {
    	this.originalTransaction = transaction.originalTransaction; 	
    	this.items = transaction.getItems();
    	this.offset = offset;
        this.x = transaction.getX();
        this.y = transaction.getY();
        this.s = transaction.getStart();
        this.e = transaction.getEnd();
	this.center = transaction.getCenter();
    }

    public Transaction(Point center, String pids){
        String[] arr = pids.split(" ");
        int n = arr.length;
        items = new Integer[n];
        for(int i = 0; i < n; i++){
            items[i] = Integer.parseInt(arr[i]);
        }
        originalTransaction = this;
        this.offset = 0;
	this.center = center;
    }

    public Transaction(Double x, Double y, String pids){
        String[] arr = pids.split(" ");
        int n = arr.length;
        items = new Integer[n];
        for(int i = 0; i < n; i++){
            items[i] = Integer.parseInt(arr[i]);
        }
        originalTransaction = this;
        this.offset = 0;
        this.x = x;
        this.y = y;
    }

    public Transaction(Double x, Double y, Integer s, Integer e, String pids){
        String[] arr = pids.split(" ");
        int n = arr.length;
        items = new Integer[n];
        for(int i = 0; i < n; i++){
            items[i] = Integer.parseInt(arr[i]);
        }
        originalTransaction = this;
        this.offset = 0;
        this.x = x;
        this.y = y;
        this.s = s;
        this.e = e;
    }

    public Integer[] getItems() {
        return items;
    }
    
    public Double getX() { return x; }
    public Double getY() { return y; }
    public Point getCenter() { return center; }
    public Integer getStart() { return s; }
    public Integer getEnd()   { return e; }
    public int getOffset() { return offset; }

    public int containsByBinarySearch(Integer item) {
	int low = offset;
	int high = items.length - 1;

	while (high >= low) {
	    int middle = ( low + high ) >>> 1; // divide by 2
	    if (items[middle].equals(item)) {
		return middle;
	    }
	    if (items[middle]< item) {
		low = middle + 1;
	    }
	    if (items[middle] > item) {
		high = middle - 1;
	    }
	}
	return -1;
    }
	
    public boolean containsByBinarySearchOriginalTransaction(Integer item) {
	Integer[] originalItems = originalTransaction.getItems();
	int low = 0;
	int high = originalItems.length - 1;

	while (high >= low) {
	    int middle = ( low + high ) >>> 1; // divide by 2
	    if (originalItems[middle].equals(item)) {
		return true;
	    }
	    if (originalItems[middle]< item) {
		low = middle + 1;
	    }
	    if (originalItems[middle] > item) {
		high = middle - 1;
	    }
	}
	return false;
    }

    @Override
    public String toString() {
        return Arrays.asList(this.items).toString();
    }

    private String itemsToString(){
        String[] str = new String[this.items.length];
        int i = 0;
        for(Integer item: items){
            str[i] = String.format("%d", item);
        }
        return String.join(" ", str);
    }

    public String asDiskString(){
        return String.format("%.3f\t%.3f\t%s", this.getX(), this.getY(), this.itemsToString());
    }

    public void removeInfrequentItems(List<Transaction>[] buckets, int minsupRelative) {
    	int i = 0;
    	for(Integer item : items) {
	    if(buckets[item].size() >= minsupRelative) {
		temp[i++] = item;
	    }
    	}
    	this.items = new Integer[i];
    	System.arraycopy(temp, 0, this.items, 0, i);
    }

    @Override
    public int compareTo(Transaction other) {
	int thisSize = this.items.length;
	int otherSize = other.items.length;
	if (thisSize < otherSize){
	    return -1;
	} else if (thisSize > otherSize){
	    return 1;
	} else {
	    for (int i = 0; i < thisSize; i++){
		if (this.items[i] < other.items[i]){
		    return -1;
		} else if (this.items[i] > other.items[i]){
		    return 1;
		}
	    }
	    return 0;
	}
    }
}
