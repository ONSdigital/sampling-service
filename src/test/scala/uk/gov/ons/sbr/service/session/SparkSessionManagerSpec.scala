package uk.gov.ons.sbr.service.session

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager

class SparkSessionManagerSpec extends FreeSpec with Matchers with MockFactory{

  private trait Fixture {
    val mockASparkMethod = mockFunction[Unit]
    val aSparkSession = TestSessionManager.newSession("SparkSessionManagerSpec")
  }

  "SparkSessionManager" - {
    "will close a SparkSession" - {
      "when the try successfully runs the method" ignore new Fixture {
        implicit val thisSparkSession = aSparkSession

//        mockASparkMethod.expects().returning()
        thisSparkSession.sparkContext.isStopped shouldBe false
        SparkSessionManager.withSpark(mockASparkMethod)
        thisSparkSession.sparkContext.isStopped shouldBe true
      }

      "when catching an Exception when trying to run the method" ignore new Fixture {
        implicit val thisSparkSession = aSparkSession

        val cause = new Exception("failed due to some cause")
        mockASparkMethod.expects().throwing(cause)
        thisSparkSession.sparkContext.isStopped shouldBe false
        val errWithMsg = the [Exception] thrownBy SparkSessionManager.withSpark(mockASparkMethod)
        errWithMsg.getMessage should startWith ("Failed to construct DataFrame when running method with error")
        thisSparkSession.sparkContext.isStopped shouldBe true
      }
    }
  }
}
