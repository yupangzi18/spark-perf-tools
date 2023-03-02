/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark.tpch

import java.io.File
import java.nio.file.{Paths, Files}
import java.util.Date
import java.time.LocalDate
import scala.util.Random
import scala.collection.mutable.HashSet

import com.codahale.metrics.{ExponentiallyDecayingReservoir, Histogram, MetricRegistry}
import com.github.mjakubowski84.parquet4s._

import org.apache.spark.SparkConf
import org.apache.spark.metrics.source.CodegenMetrics._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.BloomFilter
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.execution.benchmark.Utils._
import scala.collection.immutable.Range.BigDecimal.apply

object TPCHQueryValidator {

  private val regenerateGoldenFiles = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  val conf = new SparkConf()
      .setAppName("validate-tpch-queries")
      .set("spark.sql.parquet.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
      .set("spark.sql.autoBroadcastJoinThreshold", (20 * 1024 * 1024).toString)
      // Since Spark 3.0, `spark.sql.crossJoin.enabled` is true by default
      .set("spark.sql.crossJoin.enabled", "true")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  // spark.enableHyperspace
  import spark.implicits._

  val tables = Seq("part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region")

  val refreshTables = Seq(
    // "ilineitem"
    // "dlineitem"
    "ulineitem"
  )

  case class Lineitem(l_orderkey: Long, l_linenumber: Int, l_quantity: Double, l_extendedprice: Double,
    l_discount: Double, l_tax: Double, l_shipdate: LocalDate, pk: String, level: Int, ts: Int)
  case class PrimaryKey(pk: String)
  val sortColumns = Seq("l_orderkey", "l_linenumber", "level", "ts")

  var sbfs: List[BloomFilter] = _
  var pkLists: List[HashSet[String]] = _
  var fpCount: Int = 0

  def setupTables(dataLocation: String, method: String, format: String, refreshScaleFactor: Int): Unit = {
    // tables.foreach { tableName =>
    //   spark.read.parquet(s"$dataLocation/$tableName").createOrReplaceTempView(tableName)
    //   tableName -> spark.table(tableName).count()
    // }
    refreshTables.foreach { tableName =>
      val startTime = new Date().getTime
      var level = 0
      var tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level"
      while (Files.exists(Paths.get(tableLocation))) {
        spark.read.format(format)
          .option("inferSchema", "true")
          .option("header", "true")
          .load(s"$tableLocation/${tableName}_${level}")
          .createOrReplaceTempView(s"${tableName}_${level}")
        level += 1
        tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level"
      }
      val endTime = new Date().getTime
      println(s"Set up ${tableName}_${refreshScaleFactor}, method: $method, cost: ${endTime - startTime} ms")
    }
  }

  private lazy val genClassMetricHistogram =
    metricRegistry.histogram(MetricRegistry.name("generatedClassSize"))
  private lazy val genMethodMetricHistogram =
    metricRegistry.histogram(MetricRegistry.name("generatedMethodSize"))

  private def refreshHistogramMetrics(): Unit = {
    Seq(genClassMetricHistogram, genMethodMetricHistogram).foreach { hist =>
      val clazz = hist.getClass
      val field = clazz.getDeclaredField("reservoir")
      field.setAccessible(true)
      field.set(hist, new ExponentiallyDecayingReservoir())
    }
  }

  private def metricAsString(h: Histogram): String = {
    val metric = h.getSnapshot
    s"${metric.getMean},${metric.getMax},${metric.get75thPercentile()}"
  }

  def validateTpchTests(dataLocation: String, queries: Seq[String]): Unit = {
    setupTables(dataLocation, "baseline", "csv", 1)
    queries.foreach { name =>
      // Reset metrics for codegen
      refreshHistogramMetrics()

      val queryString = resourceToString(s"tpch/queries/$name.sql")
      val output = formatOutput(
        df = spark.sql(queryString),
        _numRows = 100000,
        truncate = Int.MaxValue,
        vertical = true
      ).trim

      val header = s"-- Automatically generated by ${getClass.getSimpleName}"

      // Read expected results from resource files
      val expectedOutput = (if (regenerateGoldenFiles) {
        val basePath = Paths.get("src", "main", "resources", "tpch", "results").toFile
        val resultFile = new File(basePath, s"$name.sql.out")
        val goldenOutput =
          s"""$header
             |$output
           """.stripMargin
        val parent = resultFile.getParentFile
        if (!parent.exists()) {
          assert(parent.mkdirs(), "Could not create directory: " + parent)
        }
        stringToFile(resultFile, goldenOutput)
        fileToString(resultFile)
      } else {
        resourceToString(s"tpch/results/$name.sql.out")
      }).replace(s"$header\n", "").trim

      // scalastyle:off println
      println(s"===== TPCH QUERY VALIDATION RESULTS FOR $name =====")
      if (expectedOutput == output) {
        println("- result check: PASSED")
      } else {
        println(
          s"""- result check: FAILED
             |expected:
             |$expectedOutput
             |actual:
             |$output
           """.stripMargin)
      }
      println(
        s"""- codegen metrics
           |classes:${metricAsString(genClassMetricHistogram)}
           |methods:${metricAsString(genMethodMetricHistogram)}
         """.stripMargin)
      // scalastyle:on println
    }
  }

  def validateTpchRefresh(dataLocation: String, method: String, format: String, refreshScaleFactor: Int): Unit = {
    // setupTables(dataLocation, method, format)

    refreshTables.foreach { tableName =>
      var level = 0
      var tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level"

      // Timer starts
      val startTime = new Date().getTime

      val queryString = resourceToString(s"tpch/queries/scan_${level}.sql")
      var mergedData = spark.sql(queryString)
        .coalesce(1)
        .orderBy("l_orderkey", "l_linenumber", "level", "ts")
        .dropDuplicates("l_orderkey", "l_linenumber")
      // spark.sql(queryString).explain(true)
      level += 1
      tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level"

      while (Files.exists(Paths.get(tableLocation))) {
        val queryString = resourceToString(s"tpch/queries/scan_${level}.sql")
        val levelData = spark.sql(queryString)
          .coalesce(1)
          .orderBy("l_orderkey", "l_linenumber", "level", "ts")
          .dropDuplicates("l_orderkey", "l_linenumber")
        // spark.sql(queryString).explain(true)
        mergedData = mergedData.union(levelData).dropDuplicates("l_orderkey", "l_linenumber")
        level += 1
        tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level"
      }

      mergedData.collect
      // Timer ends
      val endTime = new Date().getTime
      println(s"method: $method, time spend(ms): ${endTime - startTime}")

      mergedData.orderBy("l_orderkey", "l_linenumber").write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .option("inferSchema", "true")
        .option("header", "true")
        .save(s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/result")
    }
  }

  def validateRangeScan(dataLocation: String, method: String, refreshScaleFactor: Int): Unit = {
    refreshTables.foreach { tableName =>
      println(s"tableName: $tableName, refreshScaleFactor: $refreshScaleFactor")
      var level = 0
      var tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"

      // Timer starts
      val startTime = new Date().getTime

      var mergedData = 
        ParquetReader
          .as[Lineitem]
          .filter(Col("l_shipdate") >= LocalDate.of(1994, 1, 1) && Col("l_shipdate") < LocalDate.of(1995, 1, 1)
                  && Col("l_discount") >= 0.05 && Col("l_discount") <= 0.07
                  && Col("l_quantity") < 24.00)
          .read(Path(tableLocation))
      var deletedData = 
        ParquetReader
          .as[Lineitem]
          .filter(Col("l_linenumber") === -1)
          .read(Path(tableLocation))
      level += 1
      tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"
      
      while (Files.exists(Paths.get(tableLocation))) {
        val levelData = 
          ParquetReader
          .as[Lineitem]
          .filter(Col("l_shipdate") >= LocalDate.of(1994, 1, 1) && Col("l_shipdate") < LocalDate.of(1995, 1, 1)
                  && Col("l_discount") >= 0.05 && Col("l_discount") <= 0.07
                  && Col("l_quantity") < 24.00)
          .read(Path(tableLocation))
        val levelDeletedData = 
          ParquetReader
          .as[Lineitem]
          .filter(Col("l_linenumber") === -1)
          .read(Path(tableLocation))
        mergedData = mergedData.concat(levelData)
        deletedData = deletedData.concat(levelDeletedData)
        level += 1
        tableLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      }

      // var mergedDF = mergedData.toSeq.toDF
      // var deletedDF = deletedData.toSeq.toDF

      // var res = mergedData.toSeq.sortBy(r => (r.l_orderkey, r.l_linenumber, r.level, r.ts))
      var mergedDF = mergedData.toSeq.toDF.coalesce(1)
      // println(s"The number of partitions: ${mergedDF.rdd.getNumPartitions}")
      mergedDF = method match {
        case "baseline" => mergedDF
        case "col" => mergedDF.orderBy(sortColumns.map(new Column(_)): _*)
      }
      mergedDF = mergedDF.dropDuplicates("l_orderkey", "l_linenumber")

      // mergedDF.orderBy("l_orderkey", "l_linenumber").write
      //   .format("csv")
      //   .mode(SaveMode.Overwrite)
      //   .option("inferSchema", "true")
      //   .option("header", "true")
      //   .save(s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/result")
      // sortColumns.map(println)
      // println(s"mergedDF.count: ${mergedDF.count}")
      var deletedDF = deletedData.toSeq.toDF
      // println(s"deletedDF.count: ${deletedDF.count}")

      if (deletedDF.count != 0) {
        mergedDF = mergedDF.join(deletedDF, Seq("l_orderkey"), "leftouter")
        .filter(deletedDF("l_linenumber") =!= -1
                || (deletedDF("l_linenumber") === -1 
                    && (deletedDF("level") > mergedDF("level") 
                        || (deletedDF("level") === mergedDF("level") && deletedDF("ts") > mergedDF("ts")))))
      }

      // mergedData.groupBy(Col("l_orderkey"), Col("l_linenumber")).aggregate(Col("l_orderkey"), Col("l_linenumber"))
      // println(s"mergedDF.count: ${mergedDF.count}")
      mergedDF.collect

      // Timer ends
      val endTime = new Date().getTime
      println(s"method: $method, time spend(ms): ${endTime - startTime}")
    }
  }

  def setupsetupTablesInMemory(dataLocation: String, method: String, refreshScaleFactor: Int): Unit = {
    refreshTables.foreach { tableName =>
      var maxLevel = 0
      var level = 0
      var levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"

      while (Files.exists(Paths.get(levelLocation))) {
        level += 1
        levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      }
      maxLevel = level - 1
      val levelList = (0 to maxLevel).toList.par

      // Create bloom filters for validation
      // Store pk of level 0 to MAX_LEVEL - 1 in memory
      // Both are used for validation
      var levelDFs = levelList.take(maxLevel).map { level =>
        val levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
        spark.read
          .format("parquet")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(levelLocation)
          .select("pk")
      }

      sbfs = levelDFs.map { df =>
        val expectedNumItems = df.count
        val fpp = 0.0001
        df.stat.bloomFilter($"pk", expectedNumItems, fpp)
      }.toList
      var sbfBits = 0
      for (sbf <- sbfs) {
        sbfBits += sbf.bitSize.toInt
      }
      println(s"sbfs.bits: ${sbfBits / 8} bytes")

      pkLists = levelDFs.map { df =>
        HashSet() ++ df.as[String].collect().toSet
      }.toList
      println(s"pkSets: ${SizeEstimator.estimate(pkLists)} bytes")
    }
  }

  def notContain(dataLocation: String, method: String, refreshScaleFactor: Int, tableName: String) = udf((pk: String, level: Int) => {
    var ret = true
    for (i <- 0 to level - 1) {
      if (ret && sbfs(i).mightContain(pk)) {
        // val contains = ParquetReader
        //   .as[PrimaryKey]
        //   .filter(Col("pk") === pk)
        //   .read(Path(s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$i/${tableName}_$i"))
        //   .size > 0
        // if (contains) {
        if (pkLists(i).contains(pk)) {
          ret = false
        } else {
          fpCount += 1
        }
      }
    }
    ret
  })

  def validateRangeScanTest(dataLocation: String, method: String, refreshScaleFactor: Int): Unit = {
    setupsetupTablesInMemory(dataLocation, method, refreshScaleFactor)
    refreshTables.foreach { tableName =>
      println(s"tableName: $tableName, refreshScaleFactor: $refreshScaleFactor")
      fpCount = 0
      var maxLevel = 0
      var level = 0
      var levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"

      while (Files.exists(Paths.get(levelLocation))) {
        level += 1
        levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      }
      maxLevel = level - 1
      val levelList = (0 to maxLevel).toList.par

      // Timer starts
      val startTime = new Date().getTime

      // Range query on non-primary keys
      // Baseline scans alomost every files
      // Our method skips irrelevant files
      var levelData = levelList.map { level =>
        val levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
        ParquetReader
          .as[Lineitem]
          .filter(Col("l_shipdate") >= LocalDate.of(1994, 1, 1) && Col("l_shipdate") < LocalDate.of(1995, 1, 1)
                  && Col("l_discount") >= 0.05 && Col("l_discount") <= 0.07
                  && Col("l_quantity") < 24.00
            )
          .read(Path(levelLocation))
      }
      

      // Retain the newest version of data
      var mergedDF = levelData.par.map { data =>
        var df = data.toSeq.toDF
        df = method match {
          case "baseline" => df
          case "col" => df.orderBy(sortColumns.map(new Column(_)): _*)
        }
        // println(df.rdd.getNumPartitions)
        // println(df.count)
        df
      }.reduce(_ union _)
      mergedDF = mergedDF.coalesce(1).dropDuplicates("l_orderkey", "l_linenumber")

      // Remove invalid data
      // Filtred by bloom filters in memory
      mergedDF = mergedDF.where(notContain(dataLocation, method, refreshScaleFactor, tableName)($"pk", $"level"))
      mergedDF.collect

      // Timer ends
      val endTime = new Date().getTime
      println(s"method: $method, time spend(ms): ${endTime - startTime}, count: ${mergedDF.count}, fp: $fpCount")
      mergedDF.orderBy("l_orderkey", "l_linenumber").write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .option("inferSchema", "true")
        .option("header", "true")
        .save(s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/result")
    }
  }

  def validateRangeScanTest1(dataLocation: String, method: String, refreshScaleFactor: Int): Unit = {
    setupsetupTablesInMemory(dataLocation, method, refreshScaleFactor)
    refreshTables.foreach { tableName =>
      println(s"tableName: $tableName, refreshScaleFactor: $refreshScaleFactor")
      fpCount = 0
      var maxLevel = 0
      var level = 0
      var levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"

      while (Files.exists(Paths.get(levelLocation))) {
        level += 1
        levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      }
      maxLevel = level - 1
      val levelList = (0 to maxLevel).toList.par

      // Timer starts
      val startTime = new Date().getTime

      // Range query on non-primary keys
      // Baseline scans alomost every files
      // Our method skips irrelevant files
      var levelData = levelList.map { level =>
        val levelLocation = s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
        ParquetReader
          .as[Lineitem]
          .filter(Col("l_shipdate") >= LocalDate.of(1993, 1, 1) && Col("l_shipdate") < LocalDate.of(1997, 1, 1)
                  // && Col("l_discount") >= 0.05 && Col("l_discount") <= 0.07
                  // && Col("l_quantity") < 24.00
            )
          .read(Path(levelLocation))
      }
      

      // Retain the newest version of data
      var mergedDF = levelData.par.map { data =>
        data.toSeq.toDF
      }.reduce(_ union _)
      mergedDF = mergedDF
        .repartition(16 * maxLevel, col("l_orderkey"), col("l_linenumber"))
        .sortWithinPartitions(col("l_orderkey"), col("l_linenumber"), col("level"), col("ts"))
        .withColumn("partitionID", spark_partition_id)
        .dropDuplicates("l_orderkey", "l_linenumber", "partitionID")

      // Remove invalid data
      // Filtred by bloom filters in memory
      mergedDF = mergedDF.where(notContain(dataLocation, method, refreshScaleFactor, tableName)($"pk", $"level"))
      mergedDF.collect

      // Timer ends
      val endTime = new Date().getTime
      println(s"method: $method, time spend(ms): ${endTime - startTime}, count: ${mergedDF.count}, fp: $fpCount")
      mergedDF.orderBy("l_orderkey", "l_linenumber").write
        .format("csv")
        .mode(SaveMode.Overwrite)
        .option("inferSchema", "true")
        .option("header", "true")
        .save(s"$dataLocation/$method/${tableName}_${refreshScaleFactor}/result")
    }
  }

  def validateSecondaryIndex(dataLocation: String, secondaryKey: String, refreshScaleFactor: Int): Unit = {
    refreshTables.foreach { tableName =>
      var maxLevel = 0
      var level = 0
      var levelLocation = s"$dataLocation/baseline/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_${level}"

      while (Files.exists(Paths.get(levelLocation))) {
        level += 1
        levelLocation = s"$dataLocation/baseline/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      }
      maxLevel = level - 1
      val levelList = (0 to maxLevel).toList.par

      val indexLocation = s"$dataLocation/baseline/${tableName}_${refreshScaleFactor}/secondaryIndex"
      var secondaryDF = spark.read
        .format("parquet")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(indexLocation)

      var fileList = secondaryDF.select($"filename")
        .where(secondaryDF(secondaryKey) === lit("1992-01-02"))
        .rdd
        .map(r => r.getString(0))
        .collect
        .toList

      // Timer starts
      val startTime = new Date().getTime

      var fileData = fileList.par.map { fileLocation =>
        ParquetReader
          .as[Lineitem]
          .filter(Col("l_shipdate") === LocalDate.of(1992, 1, 2))
          .read(Path(fileLocation))
      }

      // var fileData = levelList.map { level =>
      //   val levelLocation = s"$dataLocation/baseline/${tableName}_${refreshScaleFactor}/level-$level/${tableName}_$level"
      //   ParquetReader
      //     .as[Lineitem]
      //     .filter(Col("l_shipdate") === LocalDate.of(1992, 1, 2))
      //     .read(Path(levelLocation))
      // }

      var mergedDF = fileData.par.map { data =>
        data.toSeq.toDF
      }.reduce(_ union _)
      
      // Timer ends
      val endTime = new Date().getTime
      println(s"method: secondaryIndex, time spend(ms): ${endTime - startTime}, count: ${mergedDF.count}")
    }
  }

  def validateHyperspace(dataLocation: String, method: String, refreshScaleFactor: Int): Unit = {
    refreshTables.foreach { tableName =>
      var maxLevel = 2
      var level = 0
      var levelLocation = s"$dataLocation/$method/${tableName}/level-$level/${tableName}_${level}"

      val levelList = (0 to maxLevel).toList.par
      var mergedDF = levelList.map { level =>
        val levelLocation = s"$dataLocation/$method/${tableName}/level-$level/${tableName}_$level"
        spark.read
          .format("parquet")
          .option("inferSchema", "true")
          .option("header", "true")
          .load(levelLocation)
      }.reduce(_ union _)
      println(s"df.count: ${mergedDF.count}")
      println(mergedDF.schema)
      
      // val hs = new Hyperspace(spark)
      // hs.createIndex(mergedDF, IndexConfig("index1", indexedColumns = Seq("l_shipdate"), includedColumns = Seq("l_orderkey", "l_linenumber")))
      val query = mergedDF
        .filter(mergedDF("l_shipdate") >= LocalDate.of(1994, 1, 1) && mergedDF("l_shipdate") < LocalDate.of(1995, 1, 1))
        .select("l_orderkey", "l_linenumber")
      println(s"res.count: ${query.count}")
    }
  }

  def main(args: Array[String]): Unit = {
    val benchmarkArgs = new TPCHQueryValidatorArguments(args)

    // List of all TPC-H queries
    val tpchQueries = Seq(
      "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")

    // If `--query-filter` defined, filters the queries that this option selects
    val queriesToRun = if (benchmarkArgs.queryFilter.nonEmpty) {
      val queries = tpchQueries.filter { case queryName =>
        benchmarkArgs.queryFilter.contains(queryName)
      }
      if (queries.isEmpty) {
        throw new RuntimeException(
          s"Empty queries to run. Bad query name filter: ${benchmarkArgs.queryFilter}")
      }
      queries
    } else {
      tpchQueries
    }

    // var tableLocation = s"${benchmarkArgs.dataLocation}/baseline/lineitem/level-0/lineitem_0"
    // var data = spark.read
    //   .format("parquet")
    //   .option("inferSchema", "true")
    //   .option("header", "true")
    //   .load(tableLocation)
    // data.createOrReplaceTempView("lineitem")
    // println(data.schema)


    // var res = spark.sql("select l_orderkey, l_linenumber from lineitem")
    // res.collect
  
    // Seq(900).foreach { i =>
    //   validateRangeScanTest(benchmarkArgs.dataLocation, "baseline", i)
    //   validateRangeScanTest(benchmarkArgs.dataLocation, "col", i)
    // }

    // validateRangeScanTest(benchmarkArgs.dataLocation, "baseline", 900)
    // validateRangeScanTest(benchmarkArgs.dataLocation, "col", 900)

    // validateRangeScanTest1(benchmarkArgs.dataLocation, "baseline", 900)
    // validateRangeScanTest1(benchmarkArgs.dataLocation, "col", 900)

    // Seq(10, 100, 300, 500, 700, 900).foreach { i =>
    //   validateRangeScanTest1(benchmarkArgs.dataLocation, "baseline", i)
    //   validateRangeScanTest1(benchmarkArgs.dataLocation, "col", i)
    // }

    // Seq(10, 100).foreach { i =>
    //   validateRangeScanTest(benchmarkArgs.dataLocation, "baseline", i)
    //   validateRangeScanTest(benchmarkArgs.dataLocation, "col", i)
    // }

    validateSecondaryIndex(benchmarkArgs.dataLocation, "l_shipdate", 10)

    // Thread.sleep(1000000)
    spark.stop()

  }
}
