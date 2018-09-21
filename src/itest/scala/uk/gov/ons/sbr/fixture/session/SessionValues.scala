package uk.gov.ons.sbr.fixture.session

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

case class SessionValues(sparkSession: SparkSession,
                         frame: DataFrame,
                         stratificationProperties: Path,
                         outputDirectory: Path
                        )
