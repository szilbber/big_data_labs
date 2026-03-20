package com.ververica.flinktraining.exercises.datastream_scala.basics  // ← exercises!

import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase._
import com.ververica.flinktraining.exercises.datastream_java.utils.{ExerciseBase, GeoUtils, MissingSolutionException}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object RideCleansingExercise extends ExerciseBase {  // ← RideCleansingExercise + extends!
  def main(args: Array[String]) {
    val params = ParameterTool.fromArgs(args)
    val input = params.get("input", pathToRideData)

    val maxDelay = 60
    val speed = 600

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxDelay, speed)))

    val filteredRides = rides
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    printOrTest(filteredRides)

    env.execute("Taxi Ride Cleansing")
  }
}