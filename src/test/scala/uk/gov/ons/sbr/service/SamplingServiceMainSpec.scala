package uk.gov.ons.sbr.service

import org.apache.hadoop.fs.{ParentNotDirectoryException, Path}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.registers.model.CommonFrameDataFields.{cellNumber => cell_no}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._
import uk.gov.ons.sbr.helpers.TestSessionManager
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit.FieldNames._
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.{createTempDirectory, createTempFile}
import uk.gov.ons.sbr.helpers.utils.{DataTransformation, FileProcessor}
import uk.gov.ons.sbr.service.validation.SampleMethodsArguments

class SamplingServiceMainSpec extends FreeSpec with Matchers with MockFactory{

  private trait Fixture extends SampleEnterpriseUnit{
    private val TargetOutputDirectoryPrefix = "sampling_output_"
    private val EnterpriseUnitFrame = Seq(
      List(ern,         entref,      name,                           tradingStyle, address1,                  address2,        address3,   address4,                 address5, postcode, legalStatus, sic07,  employees, jobs, enterpriseTurnover, standardTurnover, groupTurnover, containedTurnover, apportionedTurnover, prn          ),
      List("1100000001","9906000015","&EAGBBROWN",                   "",           "1 HAWRIDGE HILL COTTAGES","THE VALE",      "HAWRIDGE", "CHESHAM BUCKINGHAMSHIRE","",       "HP5 3NU", "1",        "45112","1000",    "1",  "73",               "73",             "0",           "0",               "0",                 "0.109636832"),
      List("1100000002","9906000045","BUEADLIING SOLUTIONS LTD",     "",           "1 HAZELWOOD LANE",        "ABBOTS LANGLEY","",         "",                       "",       "WD5 0HA", "3",        "45180","49",      "0",  "100",              "100",            "0",           "0",               "0",                 "0.63848639" ),
      List("1100000003","9906000075","JO2WMILITED",                  "",           "1 BARRASCROFTS",          "CANONBIE",      "",         "",                       "",       "DG14 0RZ","1",        "45189","39",      "0",  "56",               "56",             "0",           "0",               "0",                 "0.095639204"),
      List("1100000004","9906000145","AUBASOT(CHRISTCHURCH) LIMITED","",           "1 GARTH EDGE",            "SHAWFORTH",     "WHITWORTH","ROCHDALE LANCASHIRE",    "",       "OL12 8EH","2",        "45320","0",       "0",  "7",                "7",              "0",           "0",               "0",                 "0.509298879")
    )

    private val StratifiedProperties = Seq (
      List(inqueryCode, cellNumber, cellDescription, selectionType, lowerClassSIC07, upperClassSIC07, lowerSizePayeEmployee, upperSizePayeEmployee, prnStartPoint, sampleSize),
      List("687",       "5819",     "&Sample",       "P",           "45111",         "45190",         "9",                   "50",                  "0.129177704", "100000"  )
    )

    val aSparkSession = TestSessionManager.sparkSession
    val frameDf = DataTransformation.toDataFrame(EnterpriseUnitFrame)(aSparkSession)
    val stratifiedPropertiesDf = DataTransformation.toDataFrame(StratifiedProperties)(aSparkSession)

    val rawOutputPath = createTempDirectory(TargetOutputDirectoryPrefix)
    val outputDirectoryPath = new Path(rawOutputPath.toString)
  }

  "A sample" - {
    "is created and exported" - {
      "when all arguments are valid and both methods are successful" in new Fixture {
        val input = SampleMethodsArguments(frameDf, stratifiedPropertiesDf, outputDirectoryPath)
        SamplingServiceMain.createSample(args = input)(aSparkSession)

        val sampleCSV = (DataTransformation.getSampleFile _).andThen(FileProcessor.lineAsListOfFields)
          .apply(rawOutputPath)

        sampleCSV shouldBe List(
          List(ern,         entref,      name,                      tradingStyle, address1,           address2,        address3, address4, address5, postcode, legalStatus, sic07,  employees, jobs, enterpriseTurnover, standardTurnover, groupTurnover, containedTurnover, apportionedTurnover, prn,           cell_no),
          List("1100000002","9906000045","BUEADLIING SOLUTIONS LTD","",           "1 HAZELWOOD LANE", "ABBOTS LANGLEY","",       "",       "",       "WD5 0HA", "3",        "45180","49",      "0",  "100",              "100",            "0",           "0",               "0",                 "0.638486390", "5819" ),
          List("1100000003","9906000075","JO2WMILITED",             "",           "1 BARRASCROFTS",   "CANONBIE",      "",       "",       "",       "DG14 0RZ","1",        "45189","39",      "0",  "56",               "56",             "0",           "0",               "0",                 "0.095639204", "5819" )
        )
      }
    }

    "is not created due to an error thrown" - {
      "when Stratification method fails due to invalid properties field (cell_no)" in new Fixture {
        val invalidStratifiedProperties = Seq (
          List(inqueryCode, cellNumber, cellDescription, selectionType, lowerClassSIC07, upperClassSIC07, lowerSizePayeEmployee, upperSizePayeEmployee, prnStartPoint, sampleSize),
          List("687",       "invalid",  "&Sample",       "P",           "45111",         "45190",         "9",                   "50",                  "0.129177704", "100000"  )
        )
        val invalidStratifiedPropertiesDf = DataTransformation.toDataFrame(invalidStratifiedProperties)(aSparkSession)

        val input = SampleMethodsArguments(frameDf, invalidStratifiedPropertiesDf, outputDirectoryPath)
        val errWithMsg = the [Exception] thrownBy SamplingServiceMain.createSample(args = input)(aSparkSession)
        errWithMsg.getMessage should include ("field (class: \"scala.Int\", name: \"cell_no\")")
      }

      "when Sampling method fails due to output directory not existing" in new Fixture {
        val badRawOutputPath = createTempFile("not_a_directory_")
        val invalidOutputDirectory = new Path(badRawOutputPath.toString)

        val input = SampleMethodsArguments(frameDf, stratifiedPropertiesDf, invalidOutputDirectory)
        val failure = the [ParentNotDirectoryException] thrownBy SamplingServiceMain.createSample(args = input)(aSparkSession)
        failure.getMessage should startWith regex s"Parent path is not a directory: .*${invalidOutputDirectory.toString}"
      }
    }
  }
}
