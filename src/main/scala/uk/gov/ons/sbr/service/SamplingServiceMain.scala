package uk.gov.ons.sbr.service

import scala.util.Try

import org.apache.spark.sql.SparkSession

import uk.gov.ons.registers.methods.{Sample, Stratification}
import uk.gov.ons.sbr.logger.SessionLogger
import uk.gov.ons.sbr.service.repository.hive.HiveUnitFrameRepository
import uk.gov.ons.sbr.service.session.SparkSessionManager
import uk.gov.ons.sbr.service.validation.{SampleMethodsArguments, ServiceValidation}
import uk.gov.ons.sbr.support.TrySupport
import uk.gov.ons.sbr.utils.Export

object SamplingServiceMain {
  def main(args: Array[String]): Unit = {
    import SparkSessionManager.sparkSession

    SparkSessionManager.withSpark{ () =>
      SessionLogger.log(msg ="Initiating Sampling Service")
      val processedArguments: SampleMethodsArguments = new ServiceValidation(HiveUnitFrameRepository)
        .validateAndParseRuntimeArgs(args = args.toList)
      SessionLogger.log(msg ="Passed validation. Beginning sample creation process..")
      createSample(processedArguments)
    }
  }

  def createSample(args: SampleMethodsArguments)(implicit sparkSession: SparkSession): Unit = {
    val stratifiedFrameDf = TrySupport.fold(Try(
      Stratification.stratification(sparkSession)
        .stratify(args.unitFrame, args.stratificationProperties)))(onFailure = err =>
      throw new Exception(s"Failed at Stratification method with error [${err.getMessage}]"), onSuccess = identity)

    SessionLogger.log(msg ="Applying stratification method process [Passed].")

    TrySupport.fold(Try(Sample.sample(sparkSession)
      .create(stratifiedFrameDf, args.stratificationProperties)))(onFailure = identity, onSuccess =
      Export(_, args.outputDirectory)
    )
  }
}
