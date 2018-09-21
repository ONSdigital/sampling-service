package uk.gov.ons.sbr.fixture

import org.apache.spark.sql.SparkSession
import org.scalatest.Outcome

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.logger.SessionLogger


trait SessionFixture extends org.scalatest.fixture.TestSuite {

  override type FixtureParam = SparkSession

  override protected def withFixture(test: OneArgTest): Outcome = {
    val appName = "Acceptance tests Sampling Service app"
    val sparkSession = TestSessionManager.newSession(appName)

    try withFixture(test.toNoArgTest(sparkSession))
    finally {
      SessionLogger.log(msg = s"Closing SparkSession [${sparkSession.sparkContext.appName}] on thread local")
      sparkSession.close
    }
  }
}
