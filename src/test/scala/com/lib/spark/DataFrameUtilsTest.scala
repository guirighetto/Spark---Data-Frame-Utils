package com.lib.spark

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.junit.Test
import org.junit.Assert._
import ai.domrock.spark.DataFrameUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.types._

@RunWith(classOf[JUnitRunner])
class DataFrameUtilsTest extends FunSuite
{
    val conf = new SparkConf()
                     .setAppName("Data Frame Test")
                     .setMaster("local")
                     .set("spark.default.parallelism","1")
    val sc = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val schemaDefault = Array(StructField("a", IntegerType, true),
                              StructField("b", IntegerType, true),
                              StructField("c", StringType, true)
                             )

    val dataDefault = Array(Row(1, 4,"bola"),
                            Row(2, 4,"ovo"),
                            Row(3, 8,"sacola")
                           )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(dataDefault),StructType(schemaDefault))

    test("Spark Drop Columns") 
    {
        val dfResult = dropColumns(df, Array("a"))

        assert(dfResult.columns.toArray === Array("b", "c"))
    }

    test("Spark Rename Map Columns") 
    {
        val dfResult = renameColumnsMap(df, Map("b" -> "foo", "c" -> "bar"))

        assert(dfResult.columns.toArray === Array("a", "foo", "bar"))
    }

    test("Spark Rename Array Columns") 
    {
        val dfResult = renameColumnsArray(df, Array("ovo", "foo", "bar"))

        assert(dfResult.columns.toArray === Array("ovo", "foo", "bar"))
    }

    test("Spark Concat - Index - 1") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", IntegerType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 4,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))
        
        val dataResult = Array(Row(1, 4,"bola"),
                               Row(2, 4,"ovo"),
                               Row(3, 8,"sacola"),
                               Row(1, 4,"bola"),
                               Row(2, 4,"ovo"),
                               Row(3, 8,"sacola")
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schema))

        val dfConcat = concatAxis(dfLeft, dfRight, axis=0)

        val dfResultFinal =  dfResult.except(dfConcat)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

     test("Spark Concat - Index - 2") 
    {
       val schemaLeft = Array(StructField("c", StringType, true),
                              StructField("b", IntegerType, true),
                              StructField("a", StringType, true)
                             )

        val dataLeft = Array(Row("bola", 4, "SIM"),
                             Row("ovo", 4, "SIM"),
                             Row("sacola", 8, "NAO")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataLeft),StructType(schemaLeft))

        val schemaRight = Array(StructField("a", StringType, true),
                                StructField("b", IntegerType, true),
                                StructField("d", IntegerType, true),
                                StructField("e", DoubleType, true)
                               )

        val dataRight = Array(Row("SIM", 4, 1, 22.33),
                              Row("NAO", 5, 2, 22.1),
                              Row("SIM", 8, 3, 903.3),
                              Row("SIM", 32, 4, 99.0)
                             )

        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schemaRight))

        val schemaResult = Array(StructField("e", DoubleType, true),
                                 StructField("a", StringType, true),
                                 StructField("b", IntegerType, true),
                                 StructField("c", StringType, true),
                                 StructField("d", IntegerType, true)
                                )

        val dataResult = Array(Row(null, "SIM", 4, "bola", null),
                               Row(null, "SIM", 4, "ovo", null),
                               Row(null, "NAO", 8, "sacola", null),
                               Row(22.33, "SIM", 4, null, 1),
                               Row(22.1, "NAO", 5, null, 2),
                               Row(903.3, "SIM", 8, null, 3),
                               Row(99.0, "SIM", 32, null, 4)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val dfConcat = concatAxis(dfLeft, dfRight, axis=0)

        val dfResultFinal =  dfResult.except(dfConcat)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Concat - Column") 
    {
        val schemaLeft = Array(StructField("a", IntegerType, true),
                               StructField("b", IntegerType, true),
                               StructField("c", StringType, true)
                              )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schemaLeft))

        val schemaRight = Array(StructField("d", IntegerType, true),
                                StructField("e", IntegerType, true),
                                StructField("f", StringType, true)
                               )

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 4,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schemaRight))

        val schemaResult = Array(StructField("a", IntegerType, true),
                                 StructField("b", IntegerType, true),
                                 StructField("c", StringType, true),
                                 StructField("d", IntegerType, true),
                                 StructField("e", IntegerType, true),
                                 StructField("f", StringType, true) 
                                )
        
        val dataResult = Array(Row(1, 4,"bola", 1, 4,"bola"),
                               Row(2, 4,"ovo", 2, 4,"ovo"),
                               Row(3, 8,"sacola", 3, 8,"sacola")
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val dfConcat = concatAxis(dfLeft, dfRight, axis=1)

        val dfResultFinal =  dfResult.except(dfConcat)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 1") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", IntegerType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 4,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))

        val schemaResult = Array(StructField("b_LEFT", IntegerType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("b_RIGHT", IntegerType, true),
                                 StructField("c_RIGHT", StringType, true),
                                 StructField("_merge", StringType, true),
                                 StructField("a", IntegerType, true)
                                )
        
        val dataResult = Array(Row(4, "bola", 4, "bola", "both", 1),
                               Row(8, "sacola", 8, "sacola", "both", 3),
                               Row(4, "ovo", 4, "ovo", "both", 2)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a"), rightOn=Array("a"), suffixes=Array("_LEFT","_RIGHT"), how="outer", indicator=true, allSuffixes=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 2") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", IntegerType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 4,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))

        val schemaResult = Array(StructField("b_LEFT", IntegerType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("b_RIGHT", IntegerType, true),
                                 StructField("c_RIGHT", StringType, true),
                                 StructField("a", IntegerType, true)
                                )
        
        val dataResult = Array(Row(4, "bola", 4, "bola", 1),
                               Row(8, "sacola", 8, "sacola", 3),
                               Row(4, "ovo", 4, "ovo", 2)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a"), rightOn=Array("a"), suffixes=Array("_LEFT","_RIGHT"), how="outer", allSuffixes=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 3") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", IntegerType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(4, 4,"ovo"),
                              Row(5, 8,"sacola")
                             )

        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))

        val schemaResult = Array(StructField("b_LEFT", IntegerType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("b_RIGHT", IntegerType, true),
                                 StructField("c_RIGHT", StringType, true),
                                 StructField("_merge", StringType, true),
                                 StructField("a", IntegerType, true)
                                )

        val dataResult = Array(Row(4, "bola", 4, "bola", "both", 1),
                               Row(8, "sacola", null, null, "left_only", 3),
                               Row(null, null, 8, "sacola", "right_only", 5),
                               Row(null, null, 4, "ovo", "right_only", 4),
                               Row(4, "ovo", null, null, "left_only", 2)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))
    
        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a"), rightOn=Array("a"), suffixes=Array("_LEFT","_RIGHT"), how="outer", indicator=true, allSuffixes=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 4") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", IntegerType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(4, 4,"bola"),
                              Row(5, 4,"ovo"),
                              Row(6, 8,"sacola")
                             )

        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))

        val schemaResult = Array(StructField("b_LEFT", IntegerType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("b_RIGHT", IntegerType, true),
                                 StructField("c_RIGHT", StringType, true),
                                 StructField("_merge", StringType, true),
                                 StructField("a", IntegerType, true)
                                )

        val dataResult = Array(Row(4, "bola", null, null, "left_only", 1),
                               Row(null, null, 8, "sacola", "right_only", 6),
                               Row(8, "sacola", null, null, "left_only", 3),
                               Row(null, null, 4, "ovo", "right_only", 5),
                               Row(null, null, 4, "bola", "right_only", 4),
                               Row(4, "ovo", null, null, "left_only", 2)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))
    
        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a"), rightOn=Array("a"), suffixes=Array("_LEFT","_RIGHT"), how="outer", indicator=true, allSuffixes=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 5") 
    {
        val schema = Array(StructField("a", IntegerType, true),
                           StructField("b", StringType, true),
                           StructField("c", StringType, true)
                          )

        val dataleft = Array(Row(1, "4","bola"),
                             Row(2, "4","ovo"),
                             Row(3, "8","sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataleft),StructType(schema))

        val dataRight = Array(Row(1, "bola", "4"),
                              Row(2, "ovo", "4"),
                              Row(3, "livro", "9000")
                             )

        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schema))

        val schemaResult = Array(StructField("b_LEFT", StringType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("b_RIGHT", StringType, true),
                                 StructField("c_RIGHT", StringType, true),
                                 StructField("_merge", StringType, true),
                                 StructField("a", IntegerType, true)
                                ) 

        val dataResult = Array(Row("4", "bola", "bola", "4", "both", 1),
                               Row("4", "ovo", "ovo", "4", "both", 2),
                               Row("8", "sacola", null, null, "left_only", 3),
                               Row(null, null, "livro", "9000", "right_only", 3)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))
    
        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a","c"), rightOn=Array("a","b"), suffixes=Array("_LEFT","_RIGHT"), how="outer", indicator=true, allSuffixes=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - Outer - 6") 
    {
        val schemaLeft = Array(StructField("a", IntegerType, true),
                               StructField("b", IntegerType, true),
                               StructField("c", StringType, true)
                              )

        val dataLeft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataLeft),StructType(schemaLeft))

        val schemaRight = Array(StructField("a", IntegerType, true),
                                StructField("d", IntegerType, true),
                                StructField("e", StringType, true)
                               )

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 4,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schemaRight))

        val schemaResult = Array(StructField("b", IntegerType, true),
                                 StructField("c", StringType, true),
                                 StructField("d", IntegerType, true),
                                 StructField("e", StringType, true),
                                 StructField("_merge", StringType, true),
                                 StructField("a", IntegerType, true)
                                )
        
        val dataResult = Array(Row(4, "bola", 4, "bola", "both", 1),
                               Row(8, "sacola", 8, "sacola", "both", 3),
                               Row(4, "ovo", 4, "ovo", "both", 2)
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val dfMerged = merge(dfLeft, dfRight, leftOn=Array("a"), rightOn=Array("a"), suffixes=Array("_LEFT","_RIGHT"), how="outer", indicator=true)

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }

    test("Spark Merge - ComplexOn - 1") 
    {
        val schemaLeft = Array(StructField("a_LEFT", IntegerType, true),
                           StructField("b_LEFT", IntegerType, true),
                           StructField("c_LEFT", StringType, true)
                          )

        val dataLeft = Array(Row(1, 4,"bola"),
                             Row(2, 4,"ovo"),
                             Row(3, 8,"sacola")
                            )

        val dfLeft = spark.createDataFrame(spark.sparkContext.parallelize(dataLeft),StructType(schemaLeft))

        val schemaRight = Array(StructField("a_RIGHT", IntegerType, true),
                                StructField("b_RIGHT", IntegerType, true),
                                StructField("c_RIGHT", StringType, true)
                               )

        val dataRight = Array(Row(1, 4,"bola"),
                              Row(2, 5,"ovo"),
                              Row(3, 8,"sacola")
                             )
        
        val dfRight = spark.createDataFrame(spark.sparkContext.parallelize(dataRight),StructType(schemaRight))


        val schemaResult = Array(StructField("a_LEFT", IntegerType, true),
                                 StructField("b_LEFT", IntegerType, true),
                                 StructField("c_LEFT", StringType, true),
                                 StructField("a_RIGHT", IntegerType, true),
                                 StructField("b_RIGHT", IntegerType, true),
                                 StructField("c_RIGHT", StringType, true)
                                )
        
        val dataResult = Array(Row(1, 4, "bola", null, null, null),
                               Row(null, null, null, 1, 4, "bola"),
                               Row(3, 8, "sacola", null, null, null),
                               Row(null, null, null, 3, 8, "sacola"),
                               Row(2, 4, "ovo", 2, 5, "ovo")
                              )

        val dfResult = spark.createDataFrame(spark.sparkContext.parallelize(dataResult),StructType(schemaResult))

        val cond1 = dfLeft("a_LEFT") === dfRight("a_RIGHT")
        val cond2 = dfLeft("b_LEFT") < dfRight("b_RIGHT")
        val condf = cond1 && cond2

        val dfMerged = merge(dfLeft, dfRight, complexOn=condf, how="outer")

        val dfResultFinal =  dfResult.except(dfMerged)

        assert(dfResultFinal.rdd.isEmpty == true)
    }
}