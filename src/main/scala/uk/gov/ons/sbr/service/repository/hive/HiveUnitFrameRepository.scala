package uk.gov.ons.sbr.service.repository.hive

import scala.util.Try

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.sbr.service.repository.UnitFrameRepository

object HiveUnitFrameRepository extends UnitFrameRepository {
  override def retrieveTableAsDataFrame(unitFrameDatabaseAndTableName: String)
     (implicit activeSession: SparkSession): Try[DataFrame] =
    Try(activeSession.sql(sqlText = s"SELECT * FROM $unitFrameDatabaseAndTableName"))
}
