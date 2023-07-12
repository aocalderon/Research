//**
//scala-cli repl --amm --ammonite-version 3.0.0-M0-19-62705f47 --scala 2.13.10 -- --class-based --tmp-output-directory
//**

case class Route(wkt: String, id: String, t1: Long, t2: Long, duration: Long)
//**

val data = spark
  .read
  .option("header", "false")
  .option("delimiter", "\t")
  .csv("file:///home/acald013/Datasets/ny_taxis/ny_taxis.wkt")
//**

val trajs = data.map{ row =>
  Route(
    row.getString(0),
    row.getString(1),
    row.getString(2).toLong,
    row.getString(3).toLong,
    row.getString(4).toLong
  )
}.cache
//**

trajs.show
trajs.count
//**
