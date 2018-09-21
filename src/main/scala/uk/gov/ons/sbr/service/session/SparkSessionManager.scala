package uk.gov.ons.sbr.service.session

import org.apache.spark.sql.SparkSession

import uk.gov.ons.sbr.logger.SessionLogger

private[service] object SparkSessionManager {
  private[session] val SparkAppName = "Registers Statistical Methods Library (SML)"

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(name = SparkAppName)
    .getOrCreate()

  def withSpark(method: () => Unit): Unit = {
    val activeSession = implicitly[SparkSession]
    val sessionName = activeSession.sparkContext.appName
    SessionLogger.log(msg = s"Running method with spark instance [$sessionName]")
    try method
    catch {
      case ex: Exception =>
        throw new Exception(s"Failed to construct DataFrame when running method with error", ex)
    }
    finally
      SessionLogger.log(msg = s"Stopping active session [$sessionName] started on " +
        s"[${activeSession.sparkContext.startTime}] in thread.")
    activeSession.close
  }
}
