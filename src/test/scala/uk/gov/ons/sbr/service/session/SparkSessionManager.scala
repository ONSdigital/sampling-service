package uk.gov.ons.sbr.service.session

import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

class SparkSessionManager extends FreeSpec with Matchers with MockFactory{

  private trait Fixture {
//    val k = new FakeFunction() {
//      override protected def onUnexpected(call: Call): Any = ???
//    }
  }

  "SparkSessionManager" - {
    "will close a SparkSession" - {
      "when the try successfully runs the method" ignore new Fixture {

      }

      "when catching an Exception when trying to run the method" ignore new Fixture {

      }
    }
  }

}
