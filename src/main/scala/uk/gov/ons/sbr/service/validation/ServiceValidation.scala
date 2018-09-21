package uk.gov.ons.sbr.service.validation

import javax.inject.Singleton

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.Validation.ErrorMessage
import uk.gov.ons.registers.{Failure, Success, Validation}
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame
import uk.gov.ons.sbr.support.HdfsSupport
import uk.gov.ons.sbr.utils.HadoopPathProcessor
import uk.gov.ons.sbr.utils.HadoopPathProcessor.Delimiter

@Singleton
class ServiceValidation(repository: UnitFrameRepository) {
  private def checkNumberOfArgsGiven(serviceArgs: List[String]): (String, String, String, String) =
    serviceArgs match {
      case unitFrameDatabaseStr :: unitFrameTableNameStr :: stratificationPropertiesStr :: outputDirectoryStr :: Nil =>
        (unitFrameDatabaseStr, unitFrameTableNameStr, stratificationPropertiesStr, outputDirectoryStr)
      case _ =>
        throw new IllegalArgumentException(s"Failed to run Sampling job due to invalid number of arguments passed " +
          s"[$serviceArgs], expected (4) got [${serviceArgs.length}]")
    }

  private def foldValidatedPath[A](pathStr: String)(parsePath: Path => Validation[ErrorMessage, A]): Validation[ErrorMessage, A] =
    Validation.fold(HadoopPathProcessor.validatePath(pathStr))(
      onFailure = err => Failure(err.mkString(Delimiter)),
      onSuccess = parsePath(_))

  private def checkProperties(pathStr: String)(implicit sparkSession: SparkSession): Validation[ErrorMessage, DataFrame] =
    foldValidatedPath(pathStr)(path =>
      Validation.fromTry(Try(HadoopPathProcessor.readCsvFileAsDataFrame(path)), onFailure = _.getMessage))

  private def checkUnitFrame(frameNameStr: String)(implicit sparkSession: SparkSession): Validation[ErrorMessage, DataFrame] =
    Validation.fromTry(repository.retrieveTableAsDataFrame(frameNameStr), onFailure =
      cause => throw new Exception(s"Failed to construct Hive Table to DataFrame", cause))

  private def checkOutputDirectory(pathStr: String)(implicit sparkSession: SparkSession): Validation[ErrorMessage, Path] =
    foldValidatedPath(pathStr)(path =>
      if (HdfsSupport.exists(path)) Success(path)
      else Failure(s"Path [$pathStr] does not resolve to an existing directory"))

  def validateAndParseRuntimeArgs(args: List[String])(implicit sparkSession: SparkSession): SampleMethodsArguments = {
    // check length of list first
    val (unitFrameDatabaseStr, unitFrameTableNameStr, stratificationPropertiesStr, outputDirectoryStr) =
      checkNumberOfArgsGiven(args)

    val unitFrameStr = HiveFrame(database = unitFrameDatabaseStr, tableName = unitFrameTableNameStr)

    val formattedArgsOrError = Validation.map3(checkUnitFrame(unitFrameStr),
      checkProperties(stratificationPropertiesStr), checkOutputDirectory(outputDirectoryStr)
    )(SampleMethodsArguments)

    Validation.fold(formattedArgsOrError)(
      errMsgs => throw new Exception(errMsgs.mkString(Delimiter)), identity)
  }
}
