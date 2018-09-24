package uk.gov.ons.sbr.fixture

import org.scalatest.Outcome

import uk.gov.ons.sbr.fixture.session.SessionInitialization
import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.logger.SessionLogger

trait SessionFixture extends org.scalatest.fixture.TestSuite {

  override type FixtureParam = SessionInitialization

  override protected def withFixture(test: OneArgTest): Outcome = {
    val appName = "Acceptance tests Sampling Service app"
    val sparkSession = TestSessionManager.newSession(appName)
    try withFixture(test.toNoArgTest(SessionInitialization(sparkSession)))
    finally {
      SessionLogger.log(msg = s"Closing SparkSession [${sparkSession.sparkContext.appName}] on thread local")
      sparkSession.close
    }
  }
}
