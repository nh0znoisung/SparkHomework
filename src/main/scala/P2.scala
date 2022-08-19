import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession 
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.{DataFrame, Column, Row}
import org.apache.spark.sql.functions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}


object P2 extends App {
	val PATH_IMPORT:  String = "src/data/2018-2010_import.csv"
	val PATH_EXPORT:  String = "src/data/2018-2010_export.csv"


	// Spark setup
	val spark = SparkSession.builder
		.master("local[*]")
		.appName("Hello Spark Scala")
		.getOrCreate()

	// Load data
	def load_data(path: String): DataFrame = {
		val df = spark.read.option("header",true)
						.option("inferSchema",true)
						.csv(path)
		return df
	}
	var df_import: DataFrame = load_data(PATH_IMPORT)
	var df_export: DataFrame = load_data(PATH_EXPORT)

	// Remove duplicate records
	df_import = df_import.dropDuplicates()
	df_export = df_export.dropDuplicates()

	// Fill NA by Median of columns value
	def impute_median(df: DataFrame, cols: Array[String]): DataFrame = {
		val imputer = new Imputer().setInputCols(cols)
								.setOutputCols(cols)
								.setStrategy("median")
		val imputed_df = imputer.fit(df).transform(df)
		return imputed_df
	}

	df_import = impute_median(df_import, Array("value"))
	df_export = impute_median(df_export, Array("value"))

	// ******************************
	// Problem 2: Accumulate value
	// Column format (There is a column year)
	var column_acc_import = df_import.withColumn("acc_value", expr("sum(value) over (partition by commodity, country order by year)"))
    column_acc_import = column_acc_import.withColumn("acc_value", column_acc_import("acc_value").cast("decimal(38,2)"))

	var column_acc_export = df_export.withColumn("acc_value", expr("sum(value) over (partition by commodity, country order by year)"))
    column_acc_export = column_acc_export.withColumn("acc_value", column_acc_export("acc_value").cast("decimal(38,2)"))

	// Row format (Each Year is a column name)
	var row_acc_import = df_import.groupBy("Commodity", "country").pivot("year").sum("value").na.fill(0)
	var row_acc_export = df_export.groupBy("Commodity", "country").pivot("year").sum("value").na.fill(0)
	for (i <- 2011 to 2018) {
		val curr_str: String = i.toString()
		val prev_str: String = (i - 1).toString()
		row_acc_import = row_acc_import.withColumn(curr_str , row_acc_import(prev_str) + row_acc_import(curr_str))
		row_acc_export = row_acc_export.withColumn(curr_str , row_acc_export(prev_str) + row_acc_export(curr_str))

	}
	for (i <- 2010 to 2018){
		val curr_str: String = i.toString()
		val prev_str: String = (i - 1).toString()
		row_acc_import = row_acc_import.withColumn(curr_str, row_acc_import(curr_str).cast("decimal(38,2)"))
		row_acc_export = row_acc_export.withColumn(curr_str, row_acc_export(curr_str).cast("decimal(38,2)"))
	}

	column_acc_import.show()
	column_acc_export.show()
	row_acc_import.show()
	row_acc_export.show()
}
