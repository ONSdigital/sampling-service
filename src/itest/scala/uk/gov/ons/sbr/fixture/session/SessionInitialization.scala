package uk.gov.ons.sbr.fixture.session

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit
import uk.gov.ons.sbr.helpers.sample.SampleEnterpriseUnit.FieldNames._
import uk.gov.ons.sbr.helpers.utils.TestFileUtils.createTempDirectory
import uk.gov.ons.sbr.helpers.utils.{DataTransformation, ExportTable}

private[fixture] case class SessionInitialization(sparkSession: SparkSession,
                                 frame: DataFrame,
                                 stratificationProperties: Path,
                                 outputDirectory: Path
                        )

private[fixture] object SessionInitialization {
  private object ArgumentValues extends SampleEnterpriseUnit{
    val EnterpriseUnitFrame =
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

    val StratifiedProperties =
      Properties(
        aFrameHeader(
          fieldNames =           inqueryCode, cellNumber, cellDescription, selectionType, lowerClassSIC07, upperClassSIC07, lowerSizePayeEmployee, upperSizePayeEmployee, prnStartPoint, sampleSize),
        aSelectionStrata(value = "687",       "5819",     "&Sample",       "P",           "45111",         "45190",         "9",                  "50",                   "0.129177704", "100000"  )
      )

    val ProvidedPropertiesPrefix = "stratification_props_"
    val TargetOutputDirectoryPrefix = "sampling_output_"
  }

  def apply(sparkSession: SparkSession): SessionInitialization = {
    val frame = DataTransformation.toDataFrame(ArgumentValues.EnterpriseUnitFrame)(sparkSession)
    val stratificationProperties = ExportTable(aListOfTableRows = ArgumentValues.StratifiedProperties,
      prefix = ArgumentValues.ProvidedPropertiesPrefix)
    val outputDirectory = createTempDirectory(ArgumentValues.TargetOutputDirectoryPrefix)
    new SessionInitialization(sparkSession, frame, stratificationProperties, outputDirectory)
  }
}
