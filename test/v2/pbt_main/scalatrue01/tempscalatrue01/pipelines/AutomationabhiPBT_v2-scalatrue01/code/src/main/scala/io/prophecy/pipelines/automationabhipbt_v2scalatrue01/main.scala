package io.prophecy.pipelines.automationabhipbt_v2scalatrue01

import io.prophecy.libs._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.config._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.functions.UDFs._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.functions.PipelineInitCode._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    create_lookup_table(context, df_s3_source_dataset)
    val df_reformat_data = reformat_data(context, df_s3_source_dataset)
    val df_select_all_from_in0 =
      select_all_from_in0(context, df_s3_source_dataset)
    val df_script_apply = script_apply(context, df_reformat_data)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AutomationabhiPBT_v2-scalatrue01")
      .config("spark.default.parallelism",             "4")
      .config("spark.sql.legacy.allowUntypedScalaUDF", "true")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomationabhiPBT_v2-scalatrue01"
    )
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/AutomationabhiPBT_v2-scalatrue01"
    ) {
      apply(context)
    }
  }

}
