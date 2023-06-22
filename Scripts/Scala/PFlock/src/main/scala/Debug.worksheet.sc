import edu.ucr.dblab.pflock.Utils
import $ivy.`org.locationtech.jts:jts-core:1.19.0`
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel, Coordinate}
import Utils._

val S = Settings(tolerance = 1e-3)

val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

val p = geofactory.createPoint(new Coordinate(2.0, 2.0))

println(p.toText())


