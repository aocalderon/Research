import $ivy.`org.apache.spark::spark-core:2.1.0`
import $ivy.`InitialDLab:simba_2.11:1.0`
import $ivy.`org.slf4j:slf4j-jdk14:1.7.25`

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

