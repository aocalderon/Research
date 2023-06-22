package dbscan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Example of how to use the DBSCAN algorithm, in source code. */
public class MainTestDBSCAN_saveToMemory {

  public static void main(String[] args) throws NumberFormatException, IOException {
	Random rand = new Random();
	List<DoubleArray> points = new ArrayList<DoubleArray>();
	for (int j = 0; j < 200; j++) {
		double [] vector = new double[2];
		for (int i = 0; i < 2; i++) { 
			double value = rand.nextDouble() * 100.0;
			vector[i] = value;
		}
		points.add(new DoubleArray(vector));
	}
    int minPts = 5;
    double epsilon = 5d;

    AlgoDBSCAN algo = new AlgoDBSCAN();

    List<Cluster> clusters = algo.run(points, minPts, epsilon);
    algo.printStatistics();

    int i = 0;
    for (Cluster cluster : clusters) {
      System.out.println("Cluster " + i++);
      for (DoubleArray dataPoint : cluster.getVectors()) {
        System.out.println("   " + dataPoint);
      }
    }
  }
}