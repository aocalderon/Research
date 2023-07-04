/**
  * Created by Fang Ziquan on 2018/7/16.
  *
  * This transformer transforms GPS(x, y) => (x, y) in meters.
  * Using Spherical Pseudo-Mercator Projection.
  * Reference: http://wiki.openstreetmap.org/wiki/Mercator
  */
  
import java.lang.Math;
public class GPSToMeter {

  public static final double RADIUS = 6378137.0; /* in meters on the equator */
  
  /* These functions take their angle parameter in degrees and return a length in meters */

  public static double lat2y(double aLat) {
    return Math.log(Math.tan(Math.PI / 4 + Math.toRadians(aLat) / 2)) * RADIUS;
  }  
  public static double lon2x(double aLong) {
    return Math.toRadians(aLong) * RADIUS;
  }
  public static void transform(double aLat, double aLong){
	Double newY = lat2y(aLat);
	Double newX = lon2x(aLong);
	//System.out.println(newY);
	//System.out.println(newX);
  }
}
