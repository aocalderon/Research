import scala.xml._

val buffer = scala.io.Source.fromFile("sample.csv")
val bikes = buffer.getLines.toList
buffer.close

val trips = bikes.map{ bike =>
 val arr = bike.split(",")
 val fromLonLat = s"${arr(2)},${arr(1)}"
 val toLonLat = s"${arr(4)},${arr(3)}"
 val id = arr(0)
 val trip = XML.loadString(s"""<trip id="${id}" type="bik_bicycle" depart="0.00" departLane="best" fromLonLat="${fromLonLat}" toLonLat="${toLonLat}"/>""")
 trip
}.foreach{println}

// duarouter --net-file ny.net.xml.gz --route-files trips.xml --output-file routes.xml --routing-threads 12 --ignore-errors
