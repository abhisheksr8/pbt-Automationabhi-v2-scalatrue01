package io.prophecy.pipelines.automationabhipbt_v2scalatrue01.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.config.Context
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.functions.UDFs._
import io.prophecy.pipelines.automationabhipbt_v2scalatrue01.functions.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object create_lookup_table {

  def apply(context: Context, in0: DataFrame): Unit =
    createLookup("LookupTest",
                 in0,
                 context.spark,
                 List("customer_id", "email"),
                 "country_code"
    )

}
