package uk.gov.ons.sbr

import scala.util.Try

import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory

import uk.gov.ons.registers.model.CommonFrameDataFields.{cellNumber => cell_no}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._
import uk.gov.ons.sbr.fixture.SessionAcceptanceSpec
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit.FieldNames._
import uk.gov.ons.sbr.helpers.utils.DataTransformation.RawTable
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.{createAPath, createTempDirectory}
import uk.gov.ons.sbr.helpers.utils.{DataTransformation, ExportTable, FileProcessor}
import uk.gov.ons.sbr.service.SamplingServiceMain
import uk.gov.ons.sbr.service.repository.UnitFrameRepository
import uk.gov.ons.sbr.service.repository.hive.HiveFrame
import uk.gov.ons.sbr.service.validation.ServiceValidation


class SamplingServiceAcceptanceSpec extends SessionAcceptanceSpec with MockFactory with SampleEnterpriseUnit{
  private val EnterpriseUnitFrame =
    aFrame(
      aFrameHeader(
        fieldNames = ern,         entref,      name,                           tradingStyle, address1,                  address2,        address3,     address4,                 address5, postcode,  legalStatus, sic07,   employees, jobs, enterpriseTurnover, standardTurnover, groupTurnover, containedTurnover, apportionedTurnover, prn          ),
      aUnit(value =  "1100000001","9906000015","&EAGBBROWN",                   NoValue,      "1 HAWRIDGE HILL COTTAGES","THE VALE",      "HAWRIDGE",   "CHESHAM BUCKINGHAMSHIRE",NoValue,  "HP5 3NU", "1",         "45112", "1000",    "1",  "73",               "73",             "0",           "0",               "0",                 "0.109636832"),
      aUnit(value =  "1100000002","9906000045","BUEADLIING SOLUTIONS LTD",     NoValue,      "1 HAZELWOOD LANE",        "ABBOTS LANGLEY",NoValue,      NoValue,                  NoValue,  "WD5 0HA", "3",         "45180", "49",      "0",  "100",              "100",            "0",           "0",               "0",                 "0.63848639" ),
      aUnit(value =  "1100000003","9906000075","JO2WMILITED",                  NoValue,      "1 BARRASCROFTS",          "CANONBIE",      NoValue,      NoValue,                  NoValue,  "DG14 0RZ","1",         "45189", "39",      "0",  "56",               "56",             "0",           "0",               "0",                 "0.095639204"),
      aUnit(value =  "1100000004","9906000145","AUBASOT(CHRISTCHURCH) LIMITED",NoValue,      "1 GARTH EDGE",            "SHAWFORTH",     "WHITWORTH",  "ROCHDALE LANCASHIRE",    NoValue,  "OL12 8EH","2",         "45320", "0",       "0",  "7",                "7",              "0",           "0",               "0",                 "0.509298879"),
      aUnit(value =  "1100000005","9906000175","HIBAER",                       NoValue,      "1 GEORGE SQUARE",         "GLASGOW",       NoValue,      NoValue,                  NoValue,  "G2 5LL",  "1",         "45177", "22",      "1",  "106",              "106",            "0",           "0",               "0",                 "0.147768898"),
      aUnit(value =  "1100000006","9906000205","HIBAER",                       NoValue,      "1 GLEN ROAD",             "HINDHEAD",      "SURREY",     NoValue,                  NoValue,  "GU26 6QE","1",         "45182", "16",      "1",  "297",              "297",            "0",           "0",               "0",                 "0.588701588"),
      aUnit(value =  "1100000007","9906000275","IBANOCTRACTS UK LTD",          NoValue,      "1 GLYNDE PLACE",          "HORSHAM",       "WEST SUSSEX",NoValue,                  NoValue,  "RH12 1NZ","1",         "46120", "2",       "2",  "287",              "287",            "0",           "0",               "0",                 "0.155647458"),
      aUnit(value =  "1100000008","9906000325","TLUBARE",                      NoValue,      "1 GORSE ROAD",            "REYDON",        "SOUTHWOLD",  NoValue,                  NoValue,  "IP18 6NQ","1",         "45130", "13",      "3",  "197",              "197",            "0",           "0",               "0",                 "0.446872271"),
      aUnit(value =  "1100000009","9906000355","BUCARR",                       NoValue,      "1 GRANVILLE AVENUE",      "LONG EATON",    "NOTTINGHAM", NoValue,                  NoValue,  "NG10 4HA","1",         "45144", "34",      "1",  "18",               "18",             "0",           "0",               "0",                 "0.847311602"),
      aUnit(value =  "1100000010","9906000405","DCAJ&WALTON",                  NoValue,      "1 GRANVILLE AVENUE",      "LONG EATON",    "NOTTINGHAM", NoValue,                  NoValue,  "NG10 4HA","1",         "46150", "1567",    "2",  "72",               "72",             "0",           "0",               "0",                 "0.548604086"),
      aUnit(value =  "1100000011","9906000415","&BAMCFLINT",                   NoValue,      "1 GARENDON WAY",          "GROBY",         "LEICESTER",  NoValue,                  NoValue,  "LE6 0YR", "1",         "45160", "19",      "0",  "400",              "400",            "0",           "0",               "0",                 "0.269071541")
    )

  private val StratifiedProperties =
    Properties(
      aFrameHeader(
        fieldNames =           inqueryCode, cellNumber, cellDescription, selectionType, lowerClassSIC07, upperClassSIC07, lowerSizePayeEmployee, upperSizePayeEmployee, prnStartPoint, sampleSize),
      aSelectionStrata(value = "687",       "5819",     "&Sample",       "P",           "45111",         "45190",         "9",                  "50",                   "0.129177704", "100000"  )
    )

  private val ProvidedPropertiesPrefix = "stratification_props_"
  private val TargetDatabase = "db"
  private val TargetTableName = "enterprise_frame"
  private val TargetUnitFrame = HiveFrame(TargetDatabase, TargetTableName)
  private val TargetOutputDirectoryPrefix = "sampling_output_123_"

  private val repository = mock[UnitFrameRepository]
  private val validation = new ServiceValidation(repository)

  info("As a Sampling Service User")
  info("I want to create a Sample from a inputted Frame based on criterias set in the properties file")
  info("So I can then retrieve the sample population from a designated file location and then analyse it")

  feature("create a Sample") {
    scenario("a frame is stratified and then sampled") { sparkSession =>

      Given("an existing hive unit frame table name")
      val listOfUnitsAsFrame = DataTransformation.toDataFrame(_: RawTable)(sparkSession)

      And("a path to a stratification properties file that exists and of correct format")
      val propertiesPath = ExportTable(aListOfTableRows = StratifiedProperties, prefix = ProvidedPropertiesPrefix)

      And("a directory to store output exists")
      val targetOutputDirectory = createTempDirectory(TargetOutputDirectoryPrefix)

      // mock hive
      (repository.retrieveTableAsDataFrame(_: TableName)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
        .returning(Try(listOfUnitsAsFrame(EnterpriseUnitFrame)))

      val samplingRuntimeArguments = List(TargetDatabase, TargetTableName, propertiesPath.toString, targetOutputDirectory.toString)
      val inputs = validation.validateAndParseRuntimeArgs(args = samplingRuntimeArguments)(sparkSession)

      When(s"the Sampling Service is invoked on a $TargetUnitFrame with ${propertiesPath.getFileName} to be stored at ${targetOutputDirectory.getFileName}")
      SamplingServiceMain.createSample(inputs)(sparkSession)

      Then(s"the sample should exist and be readable from the $targetOutputDirectory directory")
      val sampleCSV = (DataTransformation.getSampleFile _).andThen(FileProcessor.lineAsListOfFields)
        .apply(targetOutputDirectory)

      sampleCSV shouldBe
        aFrame(
          aFrameHeader(
            fieldNames = ern,         entref,      name,                      tradingStyle, address1,            address2,        address3,     address4, address5, postcode,  legalStatus, sic07,   employees, jobs, enterpriseTurnover, standardTurnover, groupTurnover, containedTurnover, apportionedTurnover, prn,          cell_no),
          aUnit(value =  "1100000005","9906000175","HIBAER",                  NoValue,      "1 GEORGE SQUARE",   "GLASGOW",       NoValue,      NoValue,  NoValue,  "G2 5LL",  "1",         "45177", "22",      "1",  "106",              "106",            "0",           "0",               "0",                 "0.147768898","5819" ),
          aUnit(value =  "1100000011","9906000415","&BAMCFLINT",              NoValue,      "1 GARENDON WAY",    "GROBY",         "LEICESTER",  NoValue,  NoValue,  "LE6 0YR", "1",         "45160", "19",      "0",  "400",              "400",            "0",           "0",               "0",                 "0.269071541","5819" ),
          aUnit(value =  "1100000008","9906000325","TLUBARE",                 NoValue,      "1 GORSE ROAD",      "REYDON",        "SOUTHWOLD",  NoValue,  NoValue,  "IP18 6NQ","1",         "45130", "13",      "3",  "197",              "197",            "0",           "0",               "0",                 "0.446872271","5819" ),
          aUnit(value =  "1100000006","9906000205","HIBAER",                  NoValue,      "1 GLEN ROAD",       "HINDHEAD",      "SURREY",     NoValue,  NoValue,  "GU26 6QE","1",         "45182", "16",      "1",  "297",              "297",            "0",           "0",               "0",                 "0.588701588","5819" ),
          aUnit(value =  "1100000002","9906000045","BUEADLIING SOLUTIONS LTD",NoValue,      "1 HAZELWOOD LANE",  "ABBOTS LANGLEY",NoValue,      NoValue,  NoValue,  "WD5 0HA", "3",         "45180", "49",      "0",  "100",              "100",            "0",           "0",               "0",                 "0.638486390","5819" ),
          aUnit(value =  "1100000009","9906000355","BUCARR",                  NoValue,      "1 GRANVILLE AVENUE","LONG EATON",    "NOTTINGHAM", NoValue,  NoValue,  "NG10 4HA","1",         "45144", "34",      "1",  "18",               "18",             "0",           "0",               "0",                 "0.847311602","5819" ),
          aUnit(value =  "1100000003","9906000075","JO2WMILITED",             NoValue,      "1 BARRASCROFTS",    "CANONBIE",      NoValue,      NoValue,  NoValue,  "DG14 0RZ","1",         "45189", "39",      "0",  "56",               "56",             "0",           "0",               "0",                 "0.095639204","5819" )
        )
    }
  }

  feature("responds when a stratified properties file cannot be found"){
    scenario("a frame is to be sampled but an exception is throw due to properties file cannot be found failure") { sparkSession =>
      Given("an existing hive unit frame table name")
      val listOfUnitsAsFrame = DataTransformation.toDataFrame(_: RawTable)(sparkSession)

      And("an invalid properties path is create from path argument")
      val badPropertiesPath = createAPath(pathStr = "invalid_stratification_props_path")

      And("a directory to store output exists")
      val targetOutputDirectory = createTempDirectory(TargetOutputDirectoryPrefix)

      // mock hive
      (repository.retrieveTableAsDataFrame(_: TableName)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
        .returning(Try(listOfUnitsAsFrame(EnterpriseUnitFrame)))

      val badArgument = List(TargetDatabase, TargetTableName, badPropertiesPath.toString, targetOutputDirectory.toString)

      val errMsg = the [Exception] thrownBy validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
      errMsg.getMessage should startWith regex s"Path does not exist: .*${badPropertiesPath.toString}.+"
    }
  }

  feature("responds when the output path argument is invalid"){
    scenario("a frame is to be sampled but fails due to the output path argument is not a directory") { sparkSession =>
      Given("an existing hive unit frame table name")
      val listOfUnitsAsFrame = DataTransformation.toDataFrame(_: RawTable)(sparkSession)

      And("a path to a stratification properties file that exists and of correct format")
      val propertiesPath = ExportTable(aListOfTableRows = StratifiedProperties, prefix = ProvidedPropertiesPrefix)

      And("output directory argument is invalid and doesn't conform to an actual directory")
      val invalidOutputDirectory = createAPath(pathStr = "invalid_directory.txt")

      // mock hive
      (repository.retrieveTableAsDataFrame(_: TableName)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
        .returning(Try(listOfUnitsAsFrame(EnterpriseUnitFrame)))

      val badArgument = List(TargetDatabase, TargetTableName, propertiesPath.toString, invalidOutputDirectory.toString)
      the [Exception] thrownBy {
        validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
      } should have message s"Path [${invalidOutputDirectory.toString}] does not resolve to an existing directory"
    }
  }

  feature("responds when the Hive frame does not exist"){
    scenario("a frame is to be sampled but the give table name doesn't exists thereby an exception is thrown") { sparkSession =>
      Given("no unit frame exists in Hive")

      And("a path to a stratification properties file that exists and of correct format")
      val propertiesPath = ExportTable(aListOfTableRows = StratifiedProperties, prefix = ProvidedPropertiesPrefix)

      And("output directory argument is invalid and doesn't conform to an actual directory")
      val targetOutputDirectory = createTempDirectory(TargetOutputDirectoryPrefix)

      // mock hive
      (repository.retrieveTableAsDataFrame(_: TableName)(_: SparkSession)).expects(TargetUnitFrame, sparkSession)
        .throwing(new Exception(s"Cannot create sql.DataFrame from given $TargetUnitFrame"))

      val badArgument = List(TargetDatabase, TargetTableName, propertiesPath.toString, targetOutputDirectory.toString)
      the [Exception] thrownBy {
        validation.validateAndParseRuntimeArgs(args = badArgument)(sparkSession)
      } should have message s"Cannot create sql.DataFrame from given $TargetUnitFrame"
    }
  }
}
