import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession 
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.{DataFrame, Column, Row}
import org.apache.spark.sql.functions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

object P1 extends App {
	val PATH_IMPORT:  String = "data/2018-2010_import.csv"
	val PATH_EXPORT:  String = "data/2018-2010_export.csv"


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
	// Problem 1: Calculate the total import and export value for each product	
	// Total import/export value and number of countries
	var new_df_import = df_import.groupBy("Commodity")
								.agg(sum("value").alias("sum_value_import"),
								count("country").alias("nums_country_import"))
	
	var new_df_export = df_export.groupBy("Commodity")
								.agg(sum("value").alias("sum_value_export"),
								count("country").alias("nums_country_export"))
	

	// Max value + Country with max value
	val max_value_import = df_import.groupBy("Commodity")
									.max("value")
									.withColumnRenamed("max(value)", "max_value")
	var tmp_import = df_import.join(max_value_import, Seq("Commodity"), "left")
	tmp_import = tmp_import.filter(tmp_import("value") === tmp_import("max_value"))
							.select("Commodity", "country", "value")
							.withColumnRenamed("country", "country_with_max_value_import")
							.withColumnRenamed("value", "max_value_import")
	
	new_df_import = new_df_import.join(tmp_import, Seq("Commodity"), "inner")
	
	val max_value_export = df_export.groupBy("Commodity")
									.max("value")
									.withColumnRenamed("max(value)", "max_value")
	var tmp_export = df_export.join(max_value_export, Seq("Commodity"), "left")
	tmp_export = tmp_export.filter(tmp_export("value") === tmp_export("max_value"))
							.select("Commodity", "country", "value")
							.withColumnRenamed("country", "country_with_max_value_export")
							.withColumnRenamed("value", "max_value_export")
	
	new_df_export = new_df_export.join(tmp_export, Seq("Commodity"), "inner")
	
	// Longest consecutive year
	def longest_consecutive_year(lst: Array[Integer]): Integer = {
		var sorted_lst: Array[Integer] = lst.sorted
        var n = lst.length
		var consecutive_year = 1
        var answer = 1
		for (i <- 1 to n - 1) {
			if (sorted_lst(i) == sorted_lst(i - 1) + 1) {
				consecutive_year += 1
			} else {
				if (consecutive_year > answer) {
					answer = consecutive_year
				}
				consecutive_year = 1
			}
		}
        if (consecutive_year > answer) {
            answer = consecutive_year
        }
		return answer
	}

	val longest_consecutive_year_udf = udf((lst: Array[Integer]) => longest_consecutive_year(lst)) 

	val year_import = df_import.select("Commodity", "year") 
                    .distinct() 
                    .groupBy("Commodity") 
                    .agg(collect_list("year")) 
                    .withColumnRenamed("collect_list(year)", "year_list")
	
	val longest_consecutive_year_import = year_import.withColumn("longest_consecutive_year_import", longest_consecutive_year_udf(col("year_list")))
                    							.drop("year_list")
	new_df_import = new_df_import.join(longest_consecutive_year_import, Seq("Commodity"), "inner") 
	
	val year_export = df_export.select("Commodity", "year") 
                    .distinct() 
                    .groupBy("Commodity") 
                    .agg(collect_list("year")) 
                    .withColumnRenamed("collect_list(year)", "year_list")
	
	val longest_consecutive_year_export = year_export.withColumn("longest_consecutive_year_export", longest_consecutive_year_udf(col("year_list")))
                    							.drop("year_list")
	new_df_export = new_df_export.join(longest_consecutive_year_export, Seq("Commodity"), "inner") 

	// Combine 2 DataFrame
	val combined_df = new_df_import.join(new_df_export, Seq("Commodity"), "inner")

	// Calculate Balance
	val balance_df = combined_df.withColumn("balance", combined_df("sum_value_export") - combined_df("sum_value_import"))
	
	combined_df.show()
	balance_df.show()

	// Write to CSV file
	val positive_balance_df = balance_df.filter(balance_df("balance") >= 0)
	val negative_balance_df = balance_df.filter(balance_df("balance") < 0)

	val DATA_DIR: String = "data/"
	val hadoopConfig: Configuration = new Configuration()
	val hdfs: FileSystem = FileSystem.get(hadoopConfig)

	def write_to_one_csv(df: DataFrame, file_name: String): Unit = {
		val CSV_FOLDER_ADDR: String = DATA_DIR + file_name
		val CSV_FILE_ADDR: String = CSV_FOLDER_ADDR + ".csv"

		// Delete folder and file csv addr if exists
		if (Files.exists(Paths.get(CSV_FOLDER_ADDR))) {
			hdfs.delete(new Path(CSV_FOLDER_ADDR), true)
			println(s"Deleted the file $CSV_FOLDER_ADDR successfully")
		}
		if (Files.exists(Paths.get(CSV_FILE_ADDR))) {
			hdfs.delete(new Path(CSV_FILE_ADDR), true)
			println(s"Deleted the file $CSV_FILE_ADDR successfully")
		}

		// merge to 1 partition + write to folder csv
		df.coalesce(1).write.csv(CSV_FOLDER_ADDR) 
		
		// Go through each file in folder + Get partition name
		val f: File = new File(CSV_FOLDER_ADDR)
		val par_name = f.listFiles.filter((f)=> f.isFile()).filter((f) => f.getName().endsWith(".csv")).toList(0).getName()
		println(par_name)

		// Move partition file outside the folder
		val path = Files.move(
			Paths.get(CSV_FOLDER_ADDR + '/' + par_name),
			Paths.get(CSV_FILE_ADDR),
			StandardCopyOption.REPLACE_EXISTING
		)

		if (path != null) {
			println(s"Moved the file $par_name successfully")
		} else {
			println(s"Could NOT move the file $par_name")
		}

		// Delete folder file_name
		hdfs.delete(new Path(CSV_FOLDER_ADDR), true)
	}
	write_to_one_csv(positive_balance_df, "positiveTradeProduct")
	write_to_one_csv(negative_balance_df, "negativeTradeProduct")
}
