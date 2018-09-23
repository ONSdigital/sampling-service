package uk.gov.ons.sbr.service.validation

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit.FieldNames._
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.{createAPath, createTempDirectory, createTempFile}
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame

class ServiceValidationSec extends FreeSpec with Matchers with MockFactory{

  private trait Fixture{
    private val ProvidedPropertiesPrefix = "stratification_props_"
    private val TargetOutputDirectoryPrefix = "sampling_output_"

    val TargetDatabase = "db"
    val TargetTableName = "enterprise_frame"
    val TargetUnitFrame = HiveFrame(TargetDatabase, TargetTableName)

    val propertiesPath = createTempFile(prefix = ProvidedPropertiesPrefix)
    val targetOutputDirectory = createTempDirectory(TargetOutputDirectoryPrefix)

    val sparkSession = TestSessionManager.sparkSession

    val repository = mock[UnitFrameRepository]
    val validation = new ServiceValidation(repository)

    val aSeqOfFrameValues = Seq(
      List(ern,         entref,      name,                           tradingStyle, address1,                  address2,        address3,   address4,                 address5, postcode, legalStatus, sic07,  employees, jobs, enterpriseTurnover, standardTurnover, groupTurnover, containedTurnover, apportionedTurnover, prn          ),
      List("1100000001","9906000015","&EAGBBROWN",                   "",           "1 HAWRIDGE HILL COTTAGES","THE VALE",      "HAWRIDGE", "CHESHAM BUCKINGHAMSHIRE","",       "HP5 3NU", "1",        "45112","1000",    "1",  "73",               "73",             "0",           "0",               "0",                 "0.109636832"),
      List("1100000002","9906000045","BUEADLIING SOLUTIONS LTD",     "",           "1 HAZELWOOD LANE",        "ABBOTS LANGLEY","",         "",                       "",       "WD5 0HA", "3",        "45180","49",      "0",  "100",              "100",            "0",           "0",               "0",                 "0.63848639" ),
      List("1100000003","9906000075","JO2WMILITED",                  "",           "1 BARRASCROFTS",          "CANONBIE",      "",         "",                       "",       "DG14 0RZ","1",        "45189","39",      "0",  "56",               "56",             "0",           "0",               "0",                 "0.095639204"),
      List("1100000004","9906000145","AUBASOT(CHRISTCHURCH) LIMITED","",           "1 GARTH EDGE",            "SHAWFORTH",     "WHITWORTH","ROCHDALE LANCASHIRE",    "",       "OL12 8EH","2",        "45320","0",       "0",  "7",                "7",              "0",           "0",               "0",                 "0.509298879")
    )
  }

  "Runtime arguments" - {
    "are evaluated" - {
      "when three arguments are given" - {
        "returns a SampleMethodsArguments when all arguments are valid" in new Fixture {
          import sparkSession.implicits._

          (repository.retrieveTableAsDataFrame(_: String)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
            .returning(Try(aSeqOfFrameValues.toDF))

          val samplingRuntimeArguments = List(TargetDatabase, TargetTableName, propertiesPath.toString, targetOutputDirectory.toString)

          validation.validateAndParseRuntimeArgs(args = samplingRuntimeArguments)(sparkSession) shouldBe
            a [SampleMethodsArguments]
        }

        "throws an exception" - {
          "when the stratification properties does not exist" in new Fixture {
            import sparkSession.implicits._

            (repository.retrieveTableAsDataFrame(_: String)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
              .returning(Try(aSeqOfFrameValues.toDF))

            val badPropertiesPath = createAPath(pathStr = "invalid_stratification_props_path")
            val badArgument = List(TargetDatabase, TargetTableName, badPropertiesPath.toString, targetOutputDirectory.toString)

            val errMsg = the [Exception] thrownBy validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
            errMsg.getMessage should startWith regex s"Path does not exist: .*${badPropertiesPath.toString}.+"
          }

          "when output directory is not a directory" in new Fixture {
            import sparkSession.implicits._

            (repository.retrieveTableAsDataFrame(_: String)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
              .returning(Try(aSeqOfFrameValues.toDF))

            val invalidOutputDirectory = createAPath(pathStr = "invalid_directory.txt")
            val badArgument = List(TargetDatabase, TargetTableName, propertiesPath.toString, invalidOutputDirectory.toString)

            the [Exception] thrownBy {
              validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
            } should have message s"Path [${invalidOutputDirectory.toString}] does not resolve to an existing directory"
          }

          "when the frame cannot be found" in new Fixture {
            (repository.retrieveTableAsDataFrame(_: String)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
              .throwing(new Exception(s"Cannot create sql.DataFrame from given $TargetUnitFrame"))

            val badArgument = List(TargetDatabase, TargetTableName, propertiesPath.toString, targetOutputDirectory.toString)

            the [Exception] thrownBy {
              validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
            } should have message s"Cannot create sql.DataFrame from given $TargetUnitFrame"
          }
        }
      }
    }

    "cannot be evaluated" - {
      "when there is less than three arguments" in new Fixture {
        val tooFewArgs = List("enterprise_2018", "tmp/props/stratification_properties.csv")

        the [IllegalArgumentException] thrownBy {
          validation.validateAndParseRuntimeArgs(args = tooFewArgs)(sparkSession)
        } should have message s"Failed to run Sampling job due to invalid number of arguments passed " +
          s"[${tooFewArgs.toString}], expected (4) got [${tooFewArgs.length}]"
      }

      "when there are more then three arguments" in new Fixture {
        val tooManyArgs = List("db", "enterprise_2018", "tmp/props/stratification_properties.csv", "/sampling/output", "/sampling/output")

        the [IllegalArgumentException] thrownBy {
          validation.validateAndParseRuntimeArgs(args = tooManyArgs)(sparkSession)
        } should have message s"Failed to run Sampling job due to invalid number of arguments passed " +
          s"[${tooManyArgs.toString}], expected (4) got [${tooManyArgs.length}]"

      }
    }
  }

}
