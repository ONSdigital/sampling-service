package uk.gov.ons.sbr.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}

import uk.gov.ons.sbr.logger.SessionLogger
import uk.gov.ons.sbr.utils.HadoopPathProcessor.{CSV, Header}

object Export {
  def apply(dataFrame: DataFrame, path: Path, headerOption: Boolean = true): Unit = {

    val dataFramePrtn = dataFrame.coalesce(1)
    SessionLogger.log(msg = s"Exporting Sample output to csv [$path] with length [${dataFramePrtn.count}]")

    dataFramePrtn
      .write.format(CSV)
      .option(Header, headerOption)
      .mode(SaveMode.Append)
      .csv(path.toString)
  }
}
