package uk.gov.ons.sbr.service.repository.hive

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import uk.gov.ons.api.java.methods.registers.annotation.Unused
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.UnitFrameRepository.ErrorMessage
import uk.gov.ons.sbr.support.TrySupport

import scala.util.Try



trait HiveUnitFrameRepository extends UnitFrameRepository with Serializable{
  override def retrieveTableAsDataFrame(unitFrameDatabaseAndTableName: String)
     (implicit activeSession: SparkSession): Try[DataFrame] =
    Try(activeSession.sql(sqlText = s"SELECT * FROM $unitFrameDatabaseAndTableName"))

  @Unused
  def retrieveTableAsDataFrameOLD(unitFrameDatabaseAndTableName: String)
       (implicit activeSession: SparkSession): Either[ErrorMessage, Option[DataFrame]] =
    findTable(Try(activeSession.sql(sqlText = s"SELECT * FROM $unitFrameDatabaseAndTableName")))

  private def findTable(aFindTableTry: Try[DataFrame]): Either[ErrorMessage, Option[DataFrame]] =
    TrySupport.fold(aFindTableTry)(
      err => resultOnFailure(err.getMessage),
      frame => Right(Some(frame))
    )

  private def resultOnFailure(errMsg: ErrorMessage): Either[ErrorMessage, Option[DataFrame]] =
    errMsg match {
      case _ if errMsg.startsWith("No Table") => Right(None)
      case e => Left(e)
    }

  override def saveDataFrameToTable(df:DataFrame, tableName:String)(implicit activeSession: SparkSession): Try[Unit] = Try{df.write.mode(SaveMode.Overwrite).saveAsTable(tableName)}
}
//object HiveUnitFrameRepository extends HiveUnitFrameRepository