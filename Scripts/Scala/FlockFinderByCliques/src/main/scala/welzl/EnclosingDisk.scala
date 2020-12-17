/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/***
 * Based on https://github.com/apache/commons-geometry/blob/master/commons-geometry-enclosing/src/main/java/org/apache/commons/geometry/enclosing/EnclosingBall.java
 ***/

package edu.ucr.dblab.pflock.welzl

import com.vividsolutions.jts.geom.{GeometryFactory, Point}

case class EnclosingDisk(center: Point, radius: Double, support: List[Point]) {
  def contains(point: Point)
    (implicit geofactory: GeometryFactory): Boolean = {
    val tolerance = 1.0 / geofactory.getPrecisionModel.getScale
    contains(point, tolerance)
  }

  def contains(point: Point, tolerance: Double): Boolean = {
    //println("Distance: " + point.distance(center))
    //println("r + t:    " + (radius + tolerance))
    point.distance(center) <= radius + tolerance
  }

  override def toString = s"[Center: $center Radius: $radius]"
}
