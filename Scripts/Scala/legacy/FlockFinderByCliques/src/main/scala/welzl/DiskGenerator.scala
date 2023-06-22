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
 * Based on https://github.com/apache/commons-geometry/blob/master/commons-geometry-enclosing/src/main/java/org/apache/commons/geometry/enclosing/euclidean/twod/DiskGenerator.java 
 ***/

package edu.ucr.dblab.pflock.welzl

import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}
import org.apache.commons.numbers.fraction.BigFraction

object DiskGenerator {
  
  def ballOnSupport(support: List[Point])
    (implicit geofactory: GeometryFactory): EnclosingDisk = {
    val emptyPoint =
      geofactory.createPoint(new Coordinate(Double.MinValue, Double.MinValue))
    support.size match {
      case 0 => EnclosingDisk(emptyPoint, Double.MinValue, support)
      case 1 => EnclosingDisk(support.head, 0, support)
      case 2 => {
        val A = support(0)
        val B = support(1)
        val center = {
          val dx = (A.getX - B.getX) * 0.5
          val dy = (A.getY - B.getY) * 0.5
          geofactory.createPoint(new Coordinate(A.getX + dx, A.getY + dy))
        }
        val radius = A.distance(B) * 0.5

        EnclosingDisk(center, radius, support)
      }
      case 3 => {
        val vA = support(0)
        val vB = support(1)
        val vC = support(2)
        val c2 = Array(
          BigFraction.from(vA.getX()),
          BigFraction.from(vB.getX()),
          BigFraction.from(vC.getX())
        )
        val c3 = Array(
          BigFraction.from(vA.getY()),
          BigFraction.from(vB.getY()),
          BigFraction.from(vC.getY())
        )
        val c1 = Array(
          c2(0).multiply(c2(0)).add(c3(0).multiply(c3(0))),
          c2(1).multiply(c2(1)).add(c3(1).multiply(c3(1))),
          c2(2).multiply(c2(2)).add(c3(2).multiply(c3(2)))
        )
        val twoM11 = minor(c2, c3).multiply(2)
        val m12 = minor(c1, c3)
        val m13 = minor(c1, c2)
        val centerX = m12.divide(twoM11)
        val centerY = m13.divide(twoM11).negate
        val dx = c2(0).subtract(centerX)
        val dy = c3(0).subtract(centerY)
        val r2 = dx.multiply(dx).add(dy.multiply(dy))
        val coord = new Coordinate(centerX.doubleValue(), centerY.doubleValue())
        val center = geofactory.createPoint(coord)
        val radius = math.sqrt(r2.doubleValue())

        EnclosingDisk(center, radius, support)
      }
    }
  }

  private def minor(c1: Array[BigFraction], c2: Array[BigFraction]): BigFraction =  {
    c2(0).multiply(c1(2).subtract(c1(1)))
      .add(c2(1).multiply(c1(0).subtract(c1(2))))
      .add(c2(2).multiply(c1(1).subtract(c1(0))))
  }
}
