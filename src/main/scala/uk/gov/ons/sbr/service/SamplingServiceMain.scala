package uk.gov.ons.sbr.service

import org.apache.spark.sql.SparkSession
import uk.gov.ons.registers.methods.{Sample, Stratification}
import uk.gov.ons.sbr.logger.SessionLogger
import uk.gov.ons.sbr.service.session.SparkSessionManager
import uk.gov.ons.sbr.service.validation.{SampleMethodArguments, ServiceValidation, StratificationMethodArguments}
import uk.gov.ons.sbr.support.TrySupport
import uk.gov.ons.sbr.utils.DfLogger

import scala.util.Try
object SamplingServiceMain extends Stratification with ServiceValidation with DfLogger{

  /**
    * args:
    * 1. Stratification:
    *     [0] "stratification"
    *     [1] input data hive DB name
    *     [2] input data hive table name
    *     [3] path to properties csv
    *     [4] output hive DB name,
    *     [5] output hive table name,
    *     [6] unit name,
    *     [7] bounds
    *
    * 2. Sampling:
    *     [0] "sampling"
    *     [1] stratification data hive DB name
    *     [2] stratification data hive table name
    *     [3] path to properties csv
    *     [4] output hive DB name,
    *     [5] output hive table name
    * */

  def main(args: Array[String]): Unit = {
    import SparkSessionManager.sparkSession

    args(0) match{
      case uk.gov.ons.sbr.globals.Globals.sampling => SparkSessionManager.withSpark{
                                                              SessionLogger.log(msg ="Initiating Sampling Service")
                                                              val processedArguments: SampleMethodArguments = parseSamplingArgs(args.toList)
                                                              logPartition(processedArguments.stratificationDF,42,"sampling-service.main(): stratificationDF")
                                                              logPartition(processedArguments.samplingProperties,43,"sampling-service.main(): samplingProperties")
                                                               SessionLogger.log(msg ="Passed validation. Beginning sample creation process..")
                                                              createSample(processedArguments)
                                                             }

      case uk.gov.ons.sbr.globals.Globals.stratification => SparkSessionManager.withSpark{
                                                              SessionLogger.log(msg ="Initiating Sampling Service")
                                                              val processedArguments: StratificationMethodArguments = parseStratificationArgs(args.toList)
                                                              logPartition(processedArguments.unitFrame,51,"sampling-service.main(): unitFrame")
                                                              logPartition(processedArguments.stratificationProperties,52,"sampling-service.main(): stratificationProperties")
                                                              doStratify(processedArguments)
                                                             }


    }

  }

  def doStratify(args: StratificationMethodArguments)(implicit sparkSession: SparkSession): Unit = {

    val stratifiedFrameDf = TrySupport.fold(Try(
      stratify(args.unitFrame, args.stratificationProperties,args.bounds)))(onFailure = err =>
      throw new Exception(s"Failed at Stratification method with error [${err.getMessage}]"), onSuccess = identity)

   saveDataFrameToTable(stratifiedFrameDf,args.hiveTable)

    SessionLogger.log(msg ="Stratification DF saved to Hive.")
  }

  def createSample(args: SampleMethodArguments)(implicit sparkSession: SparkSession): Unit = {

    val samplesDF = TrySupport.fold(Try(Sample.sample(sparkSession)
      .create(args.stratificationDF, args.samplingProperties)))(onFailure = err =>
      throw new Exception(s"Failed at Sampling method with error [${err.getMessage}]"), onSuccess = identity)

    saveDataFrameToTable(samplesDF,args.outputTable)
    SessionLogger.log(msg ="Sampling DF saved to Hive.")

  }
}
