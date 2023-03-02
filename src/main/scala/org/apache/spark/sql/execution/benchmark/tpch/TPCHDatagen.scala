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
import scala.sys.process._
import scala.util.control._

import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.benchmark.DatagenArguments
import org.apache.spark.sql.execution.benchmark.BlockingLineStream
import org.apache.spark.sql.expressions.Window


/**
 * This class was copied from `spark-sql-perf` and modified slightly.
 */
class TPCHTables(sqlContext: SQLContext, scaleFactor: Int, var refreshScaleFactor: Int) extends Serializable {
  import sqlContext.implicits._

  private def sparkContext = sqlContext.sparkContext

  private object Table {

    def apply(name: String, partitionColumns: Seq[String], primaryKeys: Seq[String], 
              sortColumns: Seq[String], schemaString: String, bytesPerRow: Int): Table = {
      new Table(name, partitionColumns, primaryKeys, sortColumns, StructType.fromDDL(schemaString), bytesPerRow)
    }
  }

  private case class Table(name: String, partitionColumns: Seq[String], primaryKeys: Seq[String], 
                           sortColumns: Seq[String], schema: StructType, bytesPerRow: Int) {
    def nonPartitioned: Table = {
      Table(name, Nil, primaryKeys, sortColumns, schema, bytesPerRow)
    }

    /**
     *  If convertToSchema is true, the data from generator will be parsed into columns and
     *  converted to `schema`. Otherwise, it just outputs the raw data (as a single STRING column).
     */
    def df(convertToSchema: Boolean, numPartition: Int): DataFrame = {
      val partitions = if (partitionColumns.isEmpty) 1 else numPartition
      val generatedData = {
        sparkContext.parallelize(1 to partitions, partitions).flatMap { i =>
          val datagen = HdgenNative()
          println(s"dir: ${datagen.dir}, cmd: ${datagen.cmd}")
          // Note: RNGSEED is the RNG seed used by the data generator. Right now, it is fixed to 100
          val parallel = if (partitions > 1) s"-C $partitions -S $i" else ""
          val shortTableNames = Map(
            "customer" -> "c",
            "lineitem" -> "L",
            "nation" -> "n",
            "orders" -> "O",
            "part" -> "P",
            "region" -> "r",
            "supplier" -> "s",
            "partsupp" -> "S"
          )
          val commands = Seq(
            "bash", "-c",
            s"cd ${datagen.dir} && ./${datagen.cmd} -T ${shortTableNames(name)} "
            + s"-s $scaleFactor $parallel"
          )
          println(commands)
          BlockingLineStream(commands)
        }.repartition(partitions)
      }

      generatedData.setName(s"$name, sf=$scaleFactor, strings")
      println(s"generatedData.count: ${generatedData.count()}")

      // Split fields by the delimiter '|'
      // Convert seqs of string into rows
      val rows = generatedData.mapPartitions { iter =>
        iter.map { l =>
          if (convertToSchema) {
            val values = l.split("\\|", -1).dropRight(1).map { v =>
              if (v.equals("")) {
                // If the string value is an empty string, we turn it to a null
                null
              } else {
                v
              }
            }
            Row.fromSeq(values)
          } else {
            Row.fromSeq(Seq(l))
          }
        }
      }

      // Convert string data with the specified schema
      if (convertToSchema) {
        val stringData =
          sqlContext.createDataFrame(
            rows,
            StructType(schema.fields.map(f => StructField(f.name, StringType))))
        val convertedData = {
          val columns = schema.fields.map { f =>
            val expr = f.dataType match {
              // TODO: In branch-3.1, we need right-side padding for char types
              case CharType(n) => rpad(Column(f.name), n, " ")
              case _ => Column(f.name).cast(f.dataType)
            }
            expr.as(f.name)
          }
          stringData.select(columns: _*)
        }

        convertedData
      } else {
        sqlContext.createDataFrame(rows, StructType(Seq(StructField("value", StringType))))
      }
    }

    def insertDF(iFileName: String): DataFrame = {
      val datagen = HdgenNative()
      val commands = Seq(
        "bash", "-c",
        s"cd ${datagen.dir} && ./${datagen.cmd} -s $refreshScaleFactor -U 1"
      )
      println(commands)
      // Execute refresh commands in bash
      // Generate refresh files in the temp directory
      println(commands.!!)

      // Split fields by the delimiter '|'
      // Convert seqs of string into rows
      val generatedData = sqlContext.read.textFile(s"${datagen.dir}/${iFileName}").rdd
      generatedData.setName(s"$name, sf=$refreshScaleFactor, strings")
      val rows = generatedData.mapPartitions { iter =>
        iter.map{l =>
          val values = l.split("\\|", -1).dropRight(1).map { v =>
            if (v.equals("")) {
              // If the string value is an empty string, we turn it to a null
              null
            } else {
                v
            }
          }
          Row.fromSeq(values)
        }
      }

      val stringData =
        sqlContext.createDataFrame(
          rows,
          StructType(schema.fields.map(f => StructField(f.name, StringType))))
      // Convert string data with the specified schema
      val convertedData = {
        val columns = schema.fields.map { f =>
          val expr = f.dataType match {
            // TODO: In branch-3.1, we need right-side padding for char types
            case CharType(n) => rpad(Column(f.name), n, " ")
            case _ => Column(f.name).cast(f.dataType)
          }
          expr.as(f.name)
        }
        stringData.select(columns: _*)
      }
      convertedData
    }

    def deleteDF(dFileName: String): DataFrame = {
      val datagen = HdgenNative()
      val commands = Seq(
        "bash", "-c",
        s"cd ${datagen.dir} && ./${datagen.cmd} -s $refreshScaleFactor -U 1"
      )
      println(commands)
      println(commands.!!)

      val generatedData = sqlContext.read.textFile(s"${datagen.dir}/${dFileName}").rdd
      generatedData.setName(s"$name, sf=$refreshScaleFactor, strings")
      val rows = generatedData.mapPartitions { iter =>
        iter.map{l =>
          val values = l.split("\\|", -1).dropRight(1).map { v =>
            if (v.equals("")) {
              // If the string value is an empty string, we turn it to a null
              null
            } else {
              v
            }
          }
          Row.fromSeq(values)
        }
      }

      var stringData =
        sqlContext.createDataFrame(
          rows,
          StructType(StructField(schema.fields(0).name, StringType) :: Nil))
      schema.fields.drop(1).map { f =>
        stringData = stringData.withColumn(f.name, lit("-1"))
      }

      val convertedData = {
        val columns = schema.fields.map { f =>
          val expr = f.dataType match {
            // TODO: In branch-3.1, we need right-side padding for char types
            case CharType(n) => rpad(Column(f.name), n, " ")
            case _ => Column(f.name).cast(f.dataType)
          }
          expr.as(f.name)
        }
        stringData.select(columns: _*)
      }
      
      convertedData
    }

    def updateDF(
        uFileName: String,
        tableName: String, 
        method: String, 
        dataLocation: String,
        format: String): DataFrame = {
      var insertData = insertDF(uFileName)
      val insertCount = insertData.count
      val columns = insertData.columns
      
      var level = 0
      val locationPrefix = s"$dataLocation/$method/$tableName"
      var tableLocation = s"$locationPrefix/level-$level/${tableName}_${level}"

      // Find the max level
      while (Files.exists(Paths.get(tableLocation))) {
        level += 1
        tableLocation = s"$locationPrefix/level-$level/${tableName}_${level}"
      }
      level -= 1
      tableLocation = s"$locationPrefix/level-$level/${tableName}_${level}"

      // Read data from the last level
      var mergedData = sqlContext.read
        .format(format)
        .option("inferSchema", "true")
        .option("header", "true")
        .load(tableLocation)
      level -= 1
      tableLocation = s"$locationPrefix/level-$level/${tableName}_${level}"

      // Read enough data for update level by level
      // Or read all data
      while (level >= 0 && mergedData.count < insertCount) {
        var levelData = sqlContext.read
          .format(format)
          .option("inferSchema", "true")
          .option("header", "true")
          .load(tableLocation)
        mergedData = mergedData.union(levelData)

        level -= 1
        tableLocation = s"$locationPrefix/level-$level/${tableName}_${level}"
      }

      val primaryCols = primaryKeys.map(col => new Column(col))
      // Keep the same number of rows with insert data
      mergedData = mergedData.orderBy(primaryCols: _*).coalesce(1).limit(insertCount.toInt)
      val mergedRows = mergedData.rdd.zipWithIndex().map {
        case (row, id) => Row.fromSeq(id +: row.toSeq)
      }
      val insertRows = insertData.rdd.zipWithIndex().map {
        case (row, id) => Row.fromSeq(id +: row.toSeq)
      }

      mergedData = sqlContext.createDataFrame(mergedRows, StructType(StructField("id", LongType, false) +: mergedData.schema.fields))
      mergedData = mergedData.select("id", primaryKeys: _*)
      insertData = sqlContext.createDataFrame(insertRows, StructType(StructField("id", LongType, false) +: insertData.schema.fields))

      // Replace the primary keys of insert data
      // with the the primary keys of old data to simulate update
      var updateData = insertData.drop(primaryKeys: _*)
        .join(mergedData, "id")
        .drop("id")
      // Reorder columns as it used to be
      updateData = updateData.select(columns.head, columns.tail: _*)
      updateData
    }

    def useDoubleForDecimal(): Table = {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: DecimalType => DoubleType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, primaryKeys, sortColumns, StructType(newFields), bytesPerRow)
    }

    def useStringForChar(): Table = {
      val newFields = schema.fields.map { field =>
        val newDataType = field.dataType match {
          case _: CharType | _: VarcharType => StringType
          case other => other
        }
        field.copy(dataType = newDataType)
      }

      Table(name, partitionColumns, primaryKeys, sortColumns, StructType(newFields), bytesPerRow)
    }

    def genData(
        location: String,
        format: String,
        overwrite: Boolean,
        clusterByPartitionColumns: Boolean,
        filterOutNullPartitionValues: Boolean,
        numPartitions: Int,
        lsmTree: Boolean,
        maxLevel: Int,
        maxFileSize: Int,
        maxBytesForLevelBase: Int,
        maxBytesForLevelMultiplier: Int): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      println(s"numPartitions:$numPartitions")
      var data = df(format != "text", numPartitions)
      val primaryCols = primaryKeys.map(col => new Column(col))
      val sortCols = sortColumns.map(col => new Column(col))
      data = data.orderBy(primaryCols.map(_.desc): _*)
      // data.limit(50).show
      println(s"row count in $name: ${data.count()}")

      if (lsmTree) {
        var maxLevelBytes = maxBytesForLevelBase.toLong
        // maxLevelBytes / bytes per row in every table, suppose as 64 bytes here
        // In fact, we should set different values according to the table schema
        val rowsPerFile = maxFileSize / bytesPerRow
        var level = 0

        // Split single dataframe into multiple levels
        while (level < maxLevel && data.count() > 0) {
          // The number of rows in current level
          val rowCount = (maxLevelBytes / bytesPerRow).toInt
          // Get rows in current levelcoalesce
          val levelData = data.limit(rowCount)
          // Get and drop rows for each SSTable from this
          val levelLocation = s"$location/level-$level"

          var fileNum = (levelData.count() / rowsPerFile).toInt
          if (fileNum == 0) {
            fileNum = 1
          }

          println(s"$levelLocation/$name-$level")
          levelData.coalesce(1)
            .withColumn("pk", concat_ws("|", primaryCols: _*))
            .withColumn("level", lit(level).cast(IntegerType))
            .withColumn("ts", row_number.over(Window.orderBy(monotonically_increasing_id)))
            .repartitionByRange(fileNum, sortCols: _*)
            .sortWithinPartitions(sortCols: _*)
            .write
            .format(format)
            .mode(mode)
            .option("inferSchema", "true")
            .option("header", "true")
            .save(s"$levelLocation/${name}_${level}")

          level += 1
          // Drop rows that have been saved in current level
          maxLevelBytes = maxLevelBytes * maxBytesForLevelMultiplier
          val rows = data.rdd.zipWithIndex().map {
            case (row, id) => Row.fromSeq(row.toSeq :+ id)
          }
          data = sqlContext.createDataFrame(rows, StructType(data.schema.fields :+ StructField("id", LongType, false)))
          data = data.filter($"id" >= rowCount).drop("id").orderBy(primaryCols.map(_.desc): _*)
        }
      } else {
        val tempTableName = s"${name}_text"
        data.createOrReplaceTempView(tempTableName)
        val writer = if (partitionColumns.nonEmpty) {
          if (clusterByPartitionColumns) {
            val columnString = data.schema.fields.map { field =>
              field.name
            }.mkString(",")
            val partitionColumnString = partitionColumns.mkString(",")
            val predicates = if (filterOutNullPartitionValues) {
              partitionColumns.map(col => s"$col IS NOT NULL").mkString("WHERE ", " AND ", "")
            } else {
              ""
            }

            val query =
              s"""
                |SELECT
                |  $columnString
                |FROM
                |  $tempTableName
                |$predicates
                |DISTRIBUTE BY
                |  $partitionColumnString
              """.stripMargin
            val grouped = sqlContext.sql(query)
            grouped.write
          } else {
            data.write
          }
        } else {
          // If the table is not partitioned, coalesce the data to a single file.
          data.coalesce(1).write
        }
        writer.format(format).mode(mode)
        if (partitionColumns.nonEmpty) {
          writer.partitionBy(partitionColumns : _*)
        }
        writer.save(location)
        sqlContext.dropTempTable(tempTableName)
      }
    }

    def genColData(
        location: String,
        format: String,
        overwrite: Boolean,
        lsmTree: Boolean,
        maxLevel: Int = 4,
        maxFileSize: Int = 4194304,
        maxBytesForLevelBase: Int = 8388608,
        maxBytesForLevelMultiplier: Int = 10): Unit = {
      
      genData(
        location, format, overwrite, clusterByPartitionColumns = false,
        filterOutNullPartitionValues = false, numPartitions = 1, lsmTree,
        maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }

    def genRowData(
        location: String,
        format: String,
        overwrite: Boolean,
        lsmTree: Boolean,
        maxLevel: Int = 4,
        maxFileSize: Int = 4194304,
        maxBytesForLevelBase: Int = 8388608,
        maxBytesForLevelMultiplier: Int = 10): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      var level = 0
      var colTableLocation = s"$location/col/${name}_${refreshScaleFactor}/level-$level"

      val columns = schema.fields.map(f => new Column(f.name))
      val primaryCols = primaryKeys.map(col => new Column(col))
      val sortCols = primaryKeys.map(col => new Column(col))

      while (Files.exists(Paths.get(colTableLocation))) {
        // Read data from exsting column-based files
        var levelData = sqlContext.read
          .format(format)
          .option("inferSchema", "true")
          .option("header", "true")
          .load(s"$colTableLocation/${name}_${level}")
        levelData.createOrReplaceTempView(s"${name}_${level}")
        
        // Select columns of primary key
        // Add input filename as a column in the key part
        levelData = levelData.select(columns: _*)
          .withColumn("pk", concat_ws("|", primaryCols: _*))
          .withColumn("filename", input_file_name)
          .coalesce(1)
          .orderBy(primaryCols.map(_.desc): _*)

        // Get the number of file by counting exsting column-based files
        val fileNum = Option(new File(s"$colTableLocation/${name}_${level}").list).map(_.filter(_.endsWith(".parquet")).size).getOrElse(1)
        println(s"name: $name, level: $level, fileNum: $fileNum")
        val levelLocation = s"$location/row/${name}_${refreshScaleFactor}/level-$level"
        levelData.repartitionByRange(fileNum, sortCols: _*)
          .sortWithinPartitions(sortCols: _*)
          .write
          .format("csv")
          .mode(mode)
          .option("inferSchema", "true")
          .option("header", "true")
          .save(s"$levelLocation/${name}_${level}")
        level += 1
        colTableLocation = s"$location/col/${name}_${refreshScaleFactor}/level-$level"
      }
    }

    def genSecondaryIndex(
        secondaryKey: String,
        location: String,
        format: String,
        overwrite: Boolean,
        lsmTree: Boolean,
        maxLevel: Int = 4,
        maxFileSize: Int = 4194304,
        maxBytesForLevelBase: Int = 8388608,
        maxBytesForLevelMultiplier: Int = 10): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      var maxLevel = 0
      var level = 0
      var levelLocation = s"$location/baseline/${name}_${refreshScaleFactor}/level-$level"
      
      val columns = schema.fields.map(f => new Column(f.name))
      val primaryCols = primaryKeys.map(col => new Column(col))
      val secondaryCol = new Column(secondaryKey)

      while (Files.exists(Paths.get(levelLocation))) {
        level += 1
        levelLocation = s"$location/baseline/${name}_${refreshScaleFactor}/level-$level"
      }
      maxLevel = level - 1
      val levelList = (0 to maxLevel).toList.par

      var secondaryDF = levelList.map { level =>
        val levelLocation = s"$location/baseline/${name}_${refreshScaleFactor}/level-$level/${name}_${level}"
        sqlContext.read
          .format(format)
          .option("inferSchema", "true")
          .option("header", "true")
          .load(levelLocation)
      }.reduce(_ union _)

      secondaryDF = secondaryDF.withColumn("filename", input_file_name)
        .select(secondaryCol, $"pk", $"filename")
        .groupBy(secondaryCol, $"filename")
        .agg(collect_list($"pk").alias("pks"))
        // .agg(collect_list($"pk").alias("pks"), array_distinct(collect_list($"filename")).alias("files"))

      println(secondaryDF.count)
      
      secondaryDF.repartitionByRange(16, secondaryCol, $"filename")
        .sortWithinPartitions(secondaryCol, $"filename")
        .write
        .format(format)
        .mode(mode)
        .option("inferSchema", "true")
        .option("header", "true")
        .save(s"$location/baseline/${name}_${refreshScaleFactor}/secondaryIndex")
    }

    def refreshByMethod(
        refreshFunc: String,
        rFileName: String,
        baseTableName: String,
        method: String,
        location: String,
        format: String,
        overwrite: Boolean,
        lsmTree: Boolean,
        maxLevel: Int,
        maxFileSize: Int,
        maxBytesForLevelBase: Int,
        maxBytesForLevelMultiplier: Int = 10): Unit = {
      val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Ignore
      // Read refresh data from original text file
      var refreshData = refreshFunc match {
        case "insert" => insertDF(rFileName)
        case "delete" => deleteDF(rFileName)
        case "update" => updateDF(rFileName, baseTableName, method, location, format)
      }
      val primaryCols = primaryKeys.map(col => new Column(col))
      val sortCols = sortColumns.map(col => new Column(col))
      refreshData = refreshData.orderBy(primaryCols.map(_.desc): _*)
      println(s"${rFileName}.refreshData.count: ${refreshData.count}")

      if (lsmTree) {
        var maxLevelBytes = maxBytesForLevelBase.toLong
        val rowsPerFile = maxFileSize / bytesPerRow
        var level = 0

        var levelLocation = s"$location/$method/$baseTableName/level-$level"
        println(s"$levelLocation")
        while (Files.exists(Paths.get(s"$levelLocation"))) {
          // Read base data from existing files
          var baseData = sqlContext.read
            .format(format)
            .option("inferSchema", "true")
            .option("header", "true")
            .load(s"$levelLocation/${baseTableName}_${level}")
          baseData = baseData.drop("pk", "level", "ts").coalesce(1).orderBy(primaryCols.map(_.desc): _*)
          // Union base data and refresh data
          var compactedData = refreshData.union(baseData)
          // compactedData.printSchema()
          val rowCount = (maxLevelBytes / bytesPerRow).toInt
          
          // Retain some data in current level
          val levelData = compactedData.limit(rowCount)
          val rLevelLocation = s"$location/$method/${name}_${refreshScaleFactor}/level-$level"
          var fileNum = (levelData.count() / rowsPerFile).toInt
          if (fileNum == 0) {
            fileNum = 1
          }
          levelData.coalesce(1)
            .dropDuplicates(primaryKeys)
            .withColumn("pk", concat_ws("|", primaryCols: _*))
            .withColumn("level", lit(level).cast(IntegerType))
            .withColumn("ts", row_number.over(Window.orderBy(monotonically_increasing_id)))
            .repartitionByRange(fileNum.toInt, sortCols: _*)
            .sortWithinPartitions(sortCols: _*)
            .write
            .format(format)
            .mode(mode)
            .option("inferSchema", "true")
            .option("header", "true")
            .save(s"$rLevelLocation/${name}_${level}")
          
          // Remove data used in current level
          // Compact left data into next level
          val rows = compactedData.rdd.zipWithIndex().map {
            case (row, id) => Row.fromSeq(row.toSeq :+ id)
          }
          compactedData = sqlContext.createDataFrame(rows, StructType(compactedData.schema.fields :+ StructField("id", LongType, false)))
          refreshData = compactedData.filter($"id" >= rowCount).drop("id")

          level += 1
          maxLevelBytes = maxLevelBytes * maxBytesForLevelMultiplier
          levelLocation = s"$location/$method/$baseTableName/level-$level"
        }

        // Refresh incurs the increment of levels
        if (refreshData.count() != 0) {
          val rLevelLocation = s"$location/$method/${name}_${refreshScaleFactor}/level-${level}"
          var fileNum = (refreshData.count() * bytesPerRow / maxFileSize).toInt
          if (fileNum == 0) {
            fileNum = 1
          }
          refreshData.coalesce(1)
            .withColumn("pk", concat_ws("|", primaryCols: _*))
            .withColumn("level", lit(level).cast(IntegerType))
            .withColumn("ts", row_number.over(Window.orderBy(monotonically_increasing_id)))
            .repartitionByRange(fileNum, sortCols: _*)
            .sortWithinPartitions(sortCols: _*)
            .write
            .format(format)
            .mode(mode)
            .option("inferSchema", "true")
            .option("header", "true")
            .save(s"$rLevelLocation/${name}_${level}")
        }
      }
    }

    def refresh(
        method: String,
        location: String,
        format: String,
        overwrite: Boolean,
        lsmTree: Boolean,
        maxLevel: Int = 4,
        maxFileSize: Int = 4194304,
        maxBytesForLevelBase: Int = 8388608,
        maxBytesForLevelMultiplier: Int = 10): Unit = {
      val rFileName = name match {
        case "iorders" => "orders.tbl.u1"
        case "ilineitem" => "lineitem.tbl.u1"
        case "dorders" => "delete.1"
        case "dlineitem" => "delete.1"
        case "uorders" => "orders.tbl.u1"
        case "ulineitem" => "lineitem.tbl.u1"
      }
      val baseTableName = name match {
        case "iorders" => "orders"
        case "ilineitem" => "lineitem"
        case "dorders" => "orders"
        case "dlineitem" => "lineitem"
        case "uorders" => "orders"
        case "ulineitem" => "lineitem"
      }
      val refreshFunc = name match {
        case "iorders" => "insert"
        case "ilineitem" => "insert"
        case "dorders" => "delete"
        case "dlineitem" => "delete"
        case "uorders" => "update"
        case "ulineitem" => "update"
      }

      val res = method match {
        case "baseline" => 
          refreshByMethod(refreshFunc ,rFileName, baseTableName, "baseline", location, "parquet", overwrite,
            lsmTree, maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
        case "col" =>
          refreshByMethod(refreshFunc, rFileName, baseTableName, "col", location, "parquet", overwrite,
            lsmTree, maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
          // genRowData(location, format, overwrite, lsmTree, maxLevel, maxFileSize,
          //   maxBytesForLevelBase, maxBytesForLevelMultiplier) 
      }
    }

    def createExternalTable(
        location: String, format: String, databaseName: String, overwrite: Boolean): Unit = {
      val qualifiedTableName = databaseName + "." + name
      val tableExists = sqlContext.tableNames(databaseName).contains(name)
      if (overwrite) {
        sqlContext.sql(s"DROP TABLE IF EXISTS $databaseName.$name")
      }
      if (!tableExists || overwrite) {
        // In `3.0.0-preview2`, this method has been removed though,
        // the community is planning to revert it back in the 3.0 official release.
        // sqlContext.createExternalTable(qualifiedTableName, location, format)
        sqlContext.sparkSession.catalog.createTable(qualifiedTableName, location, format)
      }
    }

    def createTemporaryTable(location: String, format: String): Unit = {
      // In `3.0.0-preview2`, this method has been removed though,
      // the community is planning to revert it back in the 3.0 official release.
      // sqlContext.read.format(format).load(location).registerTempTable(name)
      sqlContext.read.format(format).load(location).createOrReplaceTempView(name)
    }
  }

  def genData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      useStringForChar: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: Set[String] = Set.empty,
      numPartitions: Int = 100,
      lsmTree: Boolean,
      maxLevel: Int = 4,
      maxFileSize: Int = 4194304,
      maxBytesForLevelBase: Int = 8388608,
      maxBytesForLevelMultiplier: Int = 10): Unit = {
    var tablesToBeGenerated = if (partitionTables) {
      tables
    } else {
      tables.map(_.nonPartitioned)
    }

    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter { case t => tableFilter.contains(t.name) }
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }

    val withSpecifiedDataType = {
      var tables = tablesToBeGenerated
      if (useDoubleForDecimal) tables = tables.map(_.useDoubleForDecimal())
      if (useStringForChar) tables = tables.map(_.useStringForChar())
      tables
    }

    withSpecifiedDataType.foreach { table =>
      val tableLocation = s"$location/baseline/${table.name}"
      table.genData(tableLocation, format, overwrite, clusterByPartitionColumns,
        filterOutNullPartitionValues, numPartitions, lsmTree, maxLevel,
        maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }
  }

  def genSecondaryIndex(
    secondaryKey: String,
    location: String,
    format: String,
    overwrite: Boolean,
    lsmTree: Boolean,
    maxLevel: Int = 4,
    maxFileSize: Int = 4194304,
    maxBytesForLevelBase: Int = 8388608,
    maxBytesForLevelMultiplier: Int = 10
  ): Unit = {
    indexedTables.foreach { table =>
      table.genSecondaryIndex(secondaryKey, location, format, overwrite, lsmTree,
        maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }
  }

  def genHybridData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      useStringForChar: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: Set[String] = Set.empty,
      numPartitions: Int = 100,
      lsmTree: Boolean,
      maxLevel: Int = 4,
      maxFileSize: Int = 4194304,
      maxBytesForLevelBase: Int = 8388608,
      maxBytesForLevelMultiplier: Int = 10): Unit = {
    var tablesToBeGenerated = colTables
    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter { case t => tableFilter.contains(t.name) }
      if (tablesToBeGenerated.isEmpty) {
        throw new RuntimeException("Bad table name filter: " + tableFilter)
      }
    }
    
    val withSpecifiedDataType = {
      var tables = tablesToBeGenerated
      if (useDoubleForDecimal) tables = tables.map(_.useDoubleForDecimal())
      if (useStringForChar) tables = tables.map(_.useStringForChar())
      tables
    }

    withSpecifiedDataType.foreach { table =>
      table.genColData(s"$location/col/${table.name}", format, overwrite, lsmTree,
        maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }

    // rowTables.foreach { table =>
    //   table.genRowData(location, format, overwrite, lsmTree,
    //     maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    // }
  }

  def genRefreshData(
      location: String,
      format: String,
      overwrite: Boolean,
      partitionTables: Boolean,
      useDoubleForDecimal: Boolean,
      useStringForChar: Boolean,
      clusterByPartitionColumns: Boolean,
      filterOutNullPartitionValues: Boolean,
      tableFilter: Set[String] = Set.empty,
      numPartitions: Int = 100,
      lsmTree: Boolean,
      maxLevel: Int = 4,
      maxFileSize: Int = 4194304,
      maxBytesForLevelBase: Int = 8388608,
      maxBytesForLevelMultiplier: Int = 10): Unit = {
    var tablesToBeGenerated = refreshTables
    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter { case t =>
        t.name match {
          case "ilineitem" => tableFilter.contains("lineitem")
          case "dlineitem" => tableFilter.contains("lineitem")
          case "ulineitem" => tableFilter.contains("lineitem")
          case "iorders" => tableFilter.contains("orders")
          case "dorders" => tableFilter.contains("orders")
          case "uorders" => tableFilter.contains("orders")
        }
      }
    }

    val withSpecifiedDataType = {
      var tables = tablesToBeGenerated
      if (useDoubleForDecimal) tables = tables.map(_.useDoubleForDecimal())
      if (useStringForChar) tables = tables.map(_.useStringForChar())
      tables
    }

    // withSpecifiedDataType.map {
    //   t => println(t.name)
    // }
    withSpecifiedDataType.foreach { table => 
      table.refresh("baseline" ,location, format, overwrite, lsmTree,
        maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }


    tablesToBeGenerated = refreshColTables
    if (tableFilter.nonEmpty) {
      tablesToBeGenerated = tablesToBeGenerated.filter { case t =>
        t.name match {
          case "ilineitem" => tableFilter.contains("lineitem")
          case "dlineitem" => tableFilter.contains("lineitem")
          case "ulineitem" => tableFilter.contains("lineitem")
          case "iorders" => tableFilter.contains("orders")
          case "dorders" => tableFilter.contains("orders")
          case "uorders" => tableFilter.contains("orders")
        }
      }
    }

    val colWithSpecifiedDataType = {
      var tables = tablesToBeGenerated
      if (useDoubleForDecimal) tables = tables.map(_.useDoubleForDecimal())
      if (useStringForChar) tables = tables.map(_.useStringForChar())
      tables
    }

    // colWithSpecifiedDataType.map {
    //   t => println(t.name)
    // }
    colWithSpecifiedDataType.foreach {table => 
      table.refresh("col" ,location, format, overwrite, lsmTree,
        maxLevel, maxFileSize, maxBytesForLevelBase, maxBytesForLevelMultiplier)
    }
  }

  private val tables = Seq(
    Table("part",
      partitionColumns = "p_partkey" :: Nil,
      primaryKeys = "p_partkey" :: Nil,
      sortColumns = "p_partkey" :: Nil,
      """
        |`p_partkey` LONG,
        |`p_name` VARCHAR(55),
        |`p_mfgr` CHAR(25),
        |`p_brand` CHAR(10),
        |`p_type` VARCHAR(25),
        |`p_size` INT,
        |`p_container` CHAR(10),
        |`p_retailprice`  DECIMAL(12,2),
        |`p_comment` VARCHAR(23)
      """.stripMargin,
      bytesPerRow = 120),
    Table("supplier",
      partitionColumns = "s_suppkey" :: Nil,
      primaryKeys = "s_suppkey" :: Nil,
      sortColumns = "s_suppkey" :: Nil,
      """
        |`s_suppkey` LONG,
        |`s_name` CHAR(25),
        |`s_address` VARCHAR(40),
        |`s_nationkey` LONG,
        |`s_phone` CHAR(15),
        |`s_acctbal` DECIMAL(12, 2),
        |`s_comment` VARCHAR(101)
      """.stripMargin,
      bytesPerRow = 140),
    Table("partsupp",
      partitionColumns = "ps_partkey" :: Nil,
      primaryKeys = "ps_partkey" :: Nil,
      sortColumns = "ps_partkey" :: Nil,
      """
        |`ps_partkey` LONG,
        |`ps_suppkey` LONG,
        |`ps_availqty` INT,
        |`ps_supplycost` DECIMAL(12, 2),
        |`ps_comment` VARCHAR(199)
      """.stripMargin,
      bytesPerRow = 150),
    Table("customer",
      partitionColumns = "c_custkey" :: Nil,
      primaryKeys = "c_custkey" :: Nil,
      sortColumns = "c_custkey" :: Nil,
      """
        |`c_custkey` LONG,
        |`c_name` VARCHAR(25),
        |`c_address` VARCHAR(40),
        |`c_nationkey` LONG,
        |`c_phone` CHAR(15),
        |`c_acctbal` DECIMAL(12, 2),
        |`c_mktsegment` CHAR(10),
        |`c_comment` VARCHAR(117)
      """.stripMargin,
      bytesPerRow = 162),
    Table("orders",
      partitionColumns = "o_orderkey" :: Nil,
      primaryKeys = "o_orderkey" :: Nil,
      sortColumns = "o_orderkey" :: Nil,
      """
        |`o_orderkey` LONG,
        |`o_custkey` LONG,
        |`o_orderstatus` CHAR(1),
        |`o_totalprice` DECIMAL(12, 2),
        |`o_orderdate`  DATE,
        |`o_orderpriority` CHAR(15),
        |`o_clerk` CHAR(15),
        |`o_shippriority` INT,
        |`o_comment` VARCHAR(79)
      """.stripMargin,
      bytesPerRow = 120),
    Table("lineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_partkey` LONG,
        |`l_suppkey` LONG,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12, 2),
        |`l_extendedprice` DECIMAL(12, 2),
        |`l_discount` DECIMAL(12, 2),
        |`l_tax` DECIMAL(12, 2),
        |`l_returnflag` CHAR(2),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
      bytesPerRow = 128)
    ,
    Table("nation",
      partitionColumns = "n_nationkey" :: Nil,
      primaryKeys = "n_nationkey" :: Nil,
      sortColumns = "n_nationkey" :: Nil,
      """
        |`n_nationkey` INT,
        |`n_name` CHAR(25),
        |`n_regionkey` LONG,
        |`n_comment` VARCHAR(152)
      """.stripMargin,
      bytesPerRow = 88),
    Table("region",
      partitionColumns = "r_regionkey" :: Nil,
      primaryKeys = "r_regionkey" :: Nil,
      sortColumns = "r_regionkey" :: Nil,
      """
        |`r_regionkey` INT,
        |`r_name` CHAR(25),
        |`r_comment` VARCHAR(152)
      """.stripMargin,
      bytesPerRow = 78)
  )

  private val indexedTables = Seq(
    Table("ulineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_partkey` LONG,
        |`l_suppkey` LONG,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12, 2),
        |`l_extendedprice` DECIMAL(12, 2),
        |`l_discount` DECIMAL(12, 2),
        |`l_tax` DECIMAL(12, 2),
        |`l_returnflag` CHAR(2),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
      bytesPerRow = 128)
  )

  private val rowTables = Seq(
    Table("part",
      partitionColumns = "p_partkey" :: Nil,
      primaryKeys = "p_partkey" :: Nil,
      sortColumns = "p_partkey" :: Nil,
      """
        |`p_partkey` LONG
      """.stripMargin,
      bytesPerRow = 8),
    Table("supplier",
      partitionColumns = "s_suppkey" :: Nil,
      primaryKeys = "s_suppkey" :: Nil,
      sortColumns = "s_suppkey" :: Nil,
      """
        |`s_suppkey` LONG
      """.stripMargin,
      bytesPerRow = 8),
    Table("partsupp",
      partitionColumns = "ps_partkey" :: Nil,
      primaryKeys = "ps_partkey" :: Nil,
      sortColumns = "ps_partkey" :: Nil,
      """
        |`ps_partkey` LONG,
        |`ps_suppkey` LONG
      """.stripMargin,
      bytesPerRow = 16),
    Table("customer",
      partitionColumns = "c_custkey" :: Nil,
      primaryKeys = "c_custkey" :: Nil,
      sortColumns = "c_custkey" :: Nil,
      """
        |`c_custkey` LONG
      """.stripMargin,
      bytesPerRow = 8),
    Table("orders",
      partitionColumns = "o_orderkey" :: Nil,
      primaryKeys = "o_orderkey" :: Nil,
      sortColumns = "o_orderkey" :: Nil,
      """
        |`o_orderkey` LONG
      """.stripMargin,
      bytesPerRow = 8),
    Table("lineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_linenumber` INT
      """.stripMargin,
      bytesPerRow = 12)
    ,
    Table("nation",
      partitionColumns = "n_nationkey" :: Nil,
      primaryKeys = "n_nationkey" :: Nil,
      sortColumns = "n_nationkey" :: Nil,
      """
        |`n_nationkey` INT
      """.stripMargin,
      bytesPerRow = 4),
    Table("region",
      partitionColumns = "r_regionkey" :: Nil,
      primaryKeys = "r_regionkey" :: Nil,
      sortColumns = "r_regionkey" :: Nil,
      """
        |`r_regionkey` INT
      """.stripMargin,
      bytesPerRow = 4)
  )

  private val colTables = Seq(
    Table("part",
      partitionColumns = "p_partkey" :: Nil,
      primaryKeys = "p_partkey" :: Nil,
      sortColumns = "p_partkey" :: Nil,
      """
        |`p_partkey` LONG,
        |`p_name` VARCHAR(55),
        |`p_mfgr` CHAR(25),
        |`p_brand` CHAR(10),
        |`p_type` VARCHAR(25),
        |`p_size` INT,
        |`p_container` CHAR(10),
        |`p_retailprice`  DECIMAL(12,2),
        |`p_comment` VARCHAR(23)
      """.stripMargin,
      bytesPerRow = 120),
    Table("supplier",
      partitionColumns = "s_suppkey" :: Nil,
      primaryKeys = "s_suppkey" :: Nil,
      sortColumns = "s_suppkey" :: Nil,
      """
        |`s_suppkey` LONG,
        |`s_name` CHAR(25),
        |`s_address` VARCHAR(40),
        |`s_nationkey` LONG,
        |`s_phone` CHAR(15),
        |`s_acctbal` DECIMAL(12, 2),
        |`s_comment` VARCHAR(101)
      """.stripMargin,
      bytesPerRow = 140),
    Table("partsupp",
      partitionColumns = "ps_partkey" :: Nil,
      primaryKeys = "ps_partkey" :: Nil,
      sortColumns = "ps_partkey" :: Nil,
      """
        |`ps_partkey` LONG,
        |`ps_suppkey` LONG,
        |`ps_availqty` INT,
        |`ps_supplycost` DECIMAL(12, 2),
        |`ps_comment` VARCHAR(199)
      """.stripMargin,
      bytesPerRow = 150),
    Table("customer",
      partitionColumns = "c_custkey" :: Nil,
      primaryKeys = "c_custkey" :: Nil,
      sortColumns = "c_custkey" :: Nil,
      """
        |`c_custkey` LONG,
        |`c_name` VARCHAR(25),
        |`c_address` VARCHAR(40),
        |`c_nationkey` LONG,
        |`c_phone` CHAR(15),
        |`c_acctbal` DECIMAL(12, 2),
        |`c_mktsegment` CHAR(10),
        |`c_comment` VARCHAR(117)
      """.stripMargin,
      bytesPerRow = 162),
    Table("orders",
      partitionColumns = "o_orderkey" :: Nil,
      primaryKeys = "o_orderkey" :: Nil,
      sortColumns = "o_orderdate" :: Nil,
      """
        |`o_orderkey` LONG,
        |`o_custkey` LONG,
        |`o_orderstatus` CHAR(1),
        |`o_totalprice` DECIMAL(12, 2),
        |`o_orderdate`  DATE,
        |`o_orderpriority` CHAR(15),
        |`o_clerk` CHAR(15),
        |`o_shippriority` INT,
        |`o_comment` VARCHAR(79)
      """.stripMargin,
      bytesPerRow = 120),
    Table("lineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_shipdate" :: "l_discount" :: "l_quantity" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_partkey` LONG,
        |`l_suppkey` LONG,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12, 2),
        |`l_extendedprice` DECIMAL(12, 2),
        |`l_discount` DECIMAL(12, 2),
        |`l_tax` DECIMAL(12, 2),
        |`l_returnflag` CHAR(2),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
      bytesPerRow = 128)
    ,
    Table("nation",
      partitionColumns = "n_nationkey" :: Nil,
      primaryKeys = "n_nationkey" :: Nil,
      sortColumns = "n_nationkey" :: Nil,
      """
        |`n_nationkey` INT,
        |`n_name` CHAR(25),
        |`n_regionkey` LONG,
        |`n_comment` VARCHAR(152)
      """.stripMargin,
      bytesPerRow = 88),
    Table("region",
      partitionColumns = "r_regionkey" :: Nil,
      primaryKeys = "r_regionkey" :: Nil,
      sortColumns = "r_regionkey" :: Nil,
      """
        |`r_regionkey` INT,
        |`r_name` CHAR(25),
        |`r_comment` VARCHAR(152)
      """.stripMargin,
      bytesPerRow = 78)
  )

  private val refreshTables = Seq(
    // Table("iorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderkey" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    // Table("ilineitem",
    //   partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
    //   sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   """
    //     |`l_orderkey` LONG,
    //     |`l_partkey` LONG,
    //     |`l_suppkey` LONG,
    //     |`l_linenumber` INT,
    //     |`l_quantity` DECIMAL(12, 2),
    //     |`l_extendedprice` DECIMAL(12, 2),
    //     |`l_discount` DECIMAL(12, 2),
    //     |`l_tax` DECIMAL(12, 2),
    //     |`l_returnflag` CHAR(2),
    //     |`l_linestatus` CHAR(1),
    //     |`l_shipdate` DATE,
    //     |`l_commitdate` DATE,
    //     |`l_receiptdate` DATE,
    //     |`l_shipinstruct` CHAR(25),
    //     |`l_shipmode` CHAR(10),
    //     |`l_comment` VARCHAR(44)
    //   """.stripMargin,
    //   bytesPerRow = 128),
    // Table("dorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderkey" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    // Table("dlineitem",
    //   partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
    //   sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   """
    //     |`l_orderkey` LONG,
    //     |`l_partkey` LONG,
    //     |`l_suppkey` LONG,
    //     |`l_linenumber` INT,
    //     |`l_quantity` DECIMAL(12, 2),
    //     |`l_extendedprice` DECIMAL(12, 2),
    //     |`l_discount` DECIMAL(12, 2),
    //     |`l_tax` DECIMAL(12, 2),
    //     |`l_returnflag` CHAR(2),
    //     |`l_linestatus` CHAR(1),
    //     |`l_shipdate` DATE,
    //     |`l_commitdate` DATE,
    //     |`l_receiptdate` DATE,
    //     |`l_shipinstruct` CHAR(25),
    //     |`l_shipmode` CHAR(10),
    //     |`l_comment` VARCHAR(44)
    //   """.stripMargin,
    //   bytesPerRow = 128),
    // Table("uorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderkey" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    Table("ulineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_partkey` LONG,
        |`l_suppkey` LONG,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12, 2),
        |`l_extendedprice` DECIMAL(12, 2),
        |`l_discount` DECIMAL(12, 2),
        |`l_tax` DECIMAL(12, 2),
        |`l_returnflag` CHAR(2),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
      bytesPerRow = 128)
  )

  private val refreshColTables = Seq(
    // Table("iorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderdate" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    // Table("ilineitem",
    //   partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
    //   sortColumns = "l_shipdate" :: "l_discount" :: "l_quantity" :: Nil,
    //   """
    //     |`l_orderkey` LONG,
    //     |`l_partkey` LONG,
    //     |`l_suppkey` LONG,
    //     |`l_linenumber` INT,
    //     |`l_quantity` DECIMAL(12, 2),
    //     |`l_extendedprice` DECIMAL(12, 2),
    //     |`l_discount` DECIMAL(12, 2),
    //     |`l_tax` DECIMAL(12, 2),
    //     |`l_returnflag` CHAR(2),
    //     |`l_linestatus` CHAR(1),
    //     |`l_shipdate` DATE,
    //     |`l_commitdate` DATE,
    //     |`l_receiptdate` DATE,
    //     |`l_shipinstruct` CHAR(25),
    //     |`l_shipmode` CHAR(10),
    //     |`l_comment` VARCHAR(44)
    //   """.stripMargin,
    //   bytesPerRow = 128),
    // Table("dorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderdate" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    // Table("dlineitem",
    //   partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
    //   primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
    //   sortColumns = "l_shipdate" :: "l_discount" :: "l_quantity" :: Nil,
    //   """
    //     |`l_orderkey` LONG,
    //     |`l_partkey` LONG,
    //     |`l_suppkey` LONG,
    //     |`l_linenumber` INT,
    //     |`l_quantity` DECIMAL(12, 2),
    //     |`l_extendedprice` DECIMAL(12, 2),
    //     |`l_discount` DECIMAL(12, 2),
    //     |`l_tax` DECIMAL(12, 2),
    //     |`l_returnflag` CHAR(2),
    //     |`l_linestatus` CHAR(1),
    //     |`l_shipdate` DATE,
    //     |`l_commitdate` DATE,
    //     |`l_receiptdate` DATE,
    //     |`l_shipinstruct` CHAR(25),
    //     |`l_shipmode` CHAR(10),
    //     |`l_comment` VARCHAR(44)
    //   """.stripMargin,
    //   bytesPerRow = 128),
    // Table("uorders",
    //   partitionColumns = "o_orderkey" :: Nil,
    //   primaryKeys = "o_orderkey" :: Nil,
    //   sortColumns = "o_orderkey" :: Nil,
    //   """
    //     |`o_orderkey` LONG,
    //     |`o_custkey` LONG,
    //     |`o_orderstatus` CHAR(1),
    //     |`o_totalprice` DECIMAL(12, 2),
    //     |`o_orderdate`  DATE,
    //     |`o_orderpriority` CHAR(15),
    //     |`o_clerk` CHAR(15),
    //     |`o_shippriority` INT,
    //     |`o_comment` VARCHAR(79)
    //   """.stripMargin,
    //   bytesPerRow = 120),
    Table("ulineitem",
      partitionColumns = "l_orderkey" :: "l_linenumber" :: Nil,
      primaryKeys = "l_orderkey" :: "l_linenumber" :: Nil,
      sortColumns = "l_shipdate" :: "l_discount" :: "l_quantity" :: Nil,
      """
        |`l_orderkey` LONG,
        |`l_partkey` LONG,
        |`l_suppkey` LONG,
        |`l_linenumber` INT,
        |`l_quantity` DECIMAL(12, 2),
        |`l_extendedprice` DECIMAL(12, 2),
        |`l_discount` DECIMAL(12, 2),
        |`l_tax` DECIMAL(12, 2),
        |`l_returnflag` CHAR(2),
        |`l_linestatus` CHAR(1),
        |`l_shipdate` DATE,
        |`l_commitdate` DATE,
        |`l_receiptdate` DATE,
        |`l_shipinstruct` CHAR(25),
        |`l_shipmode` CHAR(10),
        |`l_comment` VARCHAR(44)
      """.stripMargin,
      bytesPerRow = 128)
  )
}

object TPCHDatagen {

  def main(args: Array[String]): Unit = {
    val datagenArgs = new DatagenArguments(args)
    val spark = SparkSession.builder.getOrCreate()
    val tpchTables = new TPCHTables(spark.sqlContext, datagenArgs.scaleFactor.toInt, datagenArgs.refreshScaleFactor.toInt)

    // tpchTables.genData(
    //   datagenArgs.outputLocation,
    //   "parquet",
    //   datagenArgs.overwrite,
    //   datagenArgs.partitionTables,
    //   datagenArgs.useDoubleForDecimal,
    //   datagenArgs.useStringForChar,
    //   datagenArgs.clusterByPartitionColumns,
    //   datagenArgs.filterOutNullPartitionValues,
    //   datagenArgs.tableFilter,
    //   datagenArgs.numPartitions.toInt,
    //   datagenArgs.lsmTree,
    //   datagenArgs.maxLevel.toInt,
    //   datagenArgs.maxFileSize.toInt,
    //   datagenArgs.maxBytesForLevelBase.toInt,
    //   datagenArgs.maxBytesForLevelMultiplier.toInt)

    // tpchTables.genHybridData(
    //   datagenArgs.outputLocation,
    //   "parquet",
    //   datagenArgs.overwrite,
    //   datagenArgs.partitionTables,
    //   datagenArgs.useDoubleForDecimal,
    //   datagenArgs.useStringForChar,
    //   datagenArgs.clusterByPartitionColumns,
    //   datagenArgs.filterOutNullPartitionValues,
    //   datagenArgs.tableFilter,
    //   datagenArgs.numPartitions.toInt,
    //   datagenArgs.lsmTree,
    //   datagenArgs.maxLevel.toInt,
    //   datagenArgs.maxFileSize.toInt,
    //   datagenArgs.maxBytesForLevelBase.toInt,
    //   datagenArgs.maxBytesForLevelMultiplier.toInt)

    // tpchTables.refreshScaleFactor = 1
    // tpchTables.genRefreshData(
    //   datagenArgs.outputLocation,
    //   datagenArgs.format,
    //   datagenArgs.overwrite,
    //   datagenArgs.partitionTables,
    //   datagenArgs.useDoubleForDecimal,
    //   datagenArgs.useStringForChar,
    //   datagenArgs.clusterByPartitionColumns,
    //   datagenArgs.filterOutNullPartitionValues,
    //   datagenArgs.tableFilter,
    //   datagenArgs.numPartitions.toInt,
    //   datagenArgs.lsmTree,
    //   datagenArgs.maxLevel.toInt,
    //   datagenArgs.maxFileSize.toInt,
    //   datagenArgs.maxBytesForLevelBase.toInt,
    //   datagenArgs.maxBytesForLevelMultiplier.toInt)

    // Seq(10, 100, 300, 500, 700, 900).foreach { i =>
    //   tpchTables.refreshScaleFactor = i
    //   println(s"tpchTables.refreshScaleFactor: ${tpchTables.refreshScaleFactor}")
    //   tpchTables.genRefreshData(
    //     datagenArgs.outputLocation,
    //     datagenArgs.format,
    //     datagenArgs.overwrite,
    //     datagenArgs.partitionTables,
    //     datagenArgs.useDoubleForDecimal,
    //     datagenArgs.useStringForChar,
    //     datagenArgs.clusterByPartitionColumns,
    //     datagenArgs.filterOutNullPartitionValues,
    //     datagenArgs.tableFilter,
    //     datagenArgs.numPartitions.toInt,
    //     datagenArgs.lsmTree,
    //     datagenArgs.maxLevel.toInt,
    //     datagenArgs.maxFileSize.toInt,
    //     datagenArgs.maxBytesForLevelBase.toInt,
    //     datagenArgs.maxBytesForLevelMultiplier.toInt)
    // }

    Seq(10).foreach { i =>
      tpchTables.refreshScaleFactor = i
      tpchTables.genSecondaryIndex(
        "l_shipdate",
        datagenArgs.outputLocation,
        datagenArgs.format,
        datagenArgs.overwrite,
        datagenArgs.lsmTree,
        datagenArgs.maxLevel.toInt,
        datagenArgs.maxFileSize.toInt,
        datagenArgs.maxBytesForLevelBase.toInt,
        datagenArgs.maxBytesForLevelMultiplier.toInt
      )
    } 
    
    spark.stop()
  }
}