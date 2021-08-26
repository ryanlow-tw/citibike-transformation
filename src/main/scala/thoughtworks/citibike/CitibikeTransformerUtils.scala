package thoughtworks.citibike
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col

import scala.math._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.udf

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession): DataFrame = {

      val UDF: UserDefinedFunction = udf((startLat: Double, startLong: Double,
                                              endLat: Double, endLong: Double) => {
        val startLatRad = startLat * Pi / 180
        val endLatRad = endLat * Pi / 180
        val latDelta = endLatRad - startLatRad
        val lonDelta = (endLong - startLong) * Pi / 180

        // refer to haversine formula
        val a = sin(latDelta/2) * sin(latDelta/2) + cos(startLatRad) * cos(endLatRad) * sin(lonDelta/2) * sin(lonDelta/2)
        val c = 2 * atan2(sqrt(a), sqrt(1-a))
        (EarthRadiusInM * c / MetersPerMile).formatted("%.2f").toDouble
      })

      dataSet.withColumn("distance",
          UDF(col("start_station_latitude"),
            col("start_station_longitude"),
            col("end_station_latitude"),
            col("end_station_longitude")))
    }
  }
}
