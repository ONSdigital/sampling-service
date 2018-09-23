package uk.gov.ons.sbr.support

import org.apache.hadoop.fs.Path
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.createAPath

class HdfsSupportSpec extends FreeSpec with Matchers{
  private trait Fixture {
    val aSparkSession = TestSessionManager.sparkSession
    val aPath = createAPath(pathStr = "temp_file.txt")
  }

  "Checking if a HDFS path exists" - {
    "can return true if path exists" in new Fixture {
      val validPath = new Path(aPath.toString)
      HdfsSupport.exists(validPath)(aSparkSession)
    }

    "can return false if path exists" in new Fixture {
      val nonExistingPath = new Path("invalid.txt")
      HdfsSupport.exists(nonExistingPath)(aSparkSession)
    }
  }
}
