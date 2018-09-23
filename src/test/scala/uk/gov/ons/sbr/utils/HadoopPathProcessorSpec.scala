package uk.gov.ons.sbr.utils

import java.io.FileWriter
import java.nio.file.Path

import org.apache.hadoop.fs.{Path => HdfsPath}
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.registers.{Failure, Success}
import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.createTempFile

class HadoopPathProcessorSpec extends FreeSpec with Matchers{

  private def writeLines(file: Path, text: String): Unit = {
    val fw = new FileWriter(file.toString, true)
    try {
      fw.write(text)
    }
    finally fw.close()
  }

  private trait Fixture {
    val aSparkSession = TestSessionManager.sparkSession
    val aPathString = "/tmp/test/file/example.csv"
  }

  "A file is read" - {
    "when a valid path is given" - {
      "with a sparkSession" in new Fixture {
        val somePath = createTempFile(prefix = "example_string")
        writeLines(somePath, s"Id,Greeting\n1,Hello World\n2,Hallo Welt")
        HadoopPathProcessor.readCsvFileAsDataFrame(new HdfsPath(somePath.toString))(aSparkSession).count shouldBe 2L
      }
    }
  }

  "A file path" - {
    "can be generated" - {
      "when path is valid" in new Fixture {
        HadoopPathProcessor.validatePath(aPathString) shouldBe Success(new HdfsPath(aPathString))
      }
    }

    "fails to be created" - {
      "when path contains invalid characters" ignore new Fixture {
        val badPathString = "/not_a_directory/"
        HadoopPathProcessor.validatePath(badPathString) shouldBe a [Failure[_]]
      }
    }
  }
}
