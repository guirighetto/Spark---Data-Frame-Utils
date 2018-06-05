
package com.lib.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import scala.collection.mutable.Stack
import scala.collection.mutable.ListBuffer

object DataFrameUtils
{
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    import spark.implicits._

    /**
    * dropColumns
    *
    * This method drop columns of Data Frame using seq.
    *
    * @param df Data Frame to be drop columns
    * @param columns Seq with name columns
    * @return a Data Frame with columns dropped
    */
    def dropColumns(df: DataFrame, columns: Seq[String]) : DataFrame =
    {
        return df.drop(columns: _*)
    }


    /**
    * renameColumnsSeq
    *
    * This method rename columns of data frame using seq.
    * The Seq with the new names must obey the order of the columns that will be replaced
    *
    * @param df Data Frame to be renamed
    * @param columns Seq with new name columns
    * @return a Data Frame with columns renamed
    */
    def renameColumnsSeq(df: DataFrame, columns: Seq[String]) : DataFrame =
    {
        return df.toDF(columns: _*)
    }

    /**
    * renameColumnsMap
    *
    * This method rename columns of data frame using map.
    * if you rename all the columns use the renameColumnsSeq
    *
    * @param df Data Frame to be renamed
    * @param columns Map with old name column to new name column
    * @return a Data Frame with columns renamed
    */
    def renameColumnsMap(df: DataFrame, columns: Map[String, String]) : DataFrame =
    {
        return columns.foldLeft(df)((dfTmp, col) => dfTmp.withColumnRenamed(col._1, col._2))
    }


    /**
    * merge
    *
    * This method performs the merge of two data frame only with equality encodings in the keys passed by parameter
    *
    * @param df1 Data Frame left to be merged
    * @param df2 Data Frame right to be merged
    * @param leftOn Keys of left Data Frame
    * @param rightOn Keys of right Data Frame
    * @param how Type of merge (Default: "outer")
    * @param sufixes List of sufixes for left and right Data Frame (Default: ["_x", "_y"])
    * @param indicator Adds a column to output Data Frame called “_merge” with information on the source of each row.
    *                  Information column is Categorical-type and takes on a value of “left_only”
    *                  for observations whose merge key only appears in ‘left’ Data Frame,
    *                  “right_only” for observations whose merge key only appears in ‘right’ Data Frame,
    *                  and “both” if the observation’s merge key is found in both
    * @return a Data Frame merged
    */
    def merge(df1: DataFrame, df2: DataFrame, leftOn: List[String], rightOn: List[String], how: String = "outer", sufixes: List[String] = List("_x","_y"), indicator: Boolean = false) : DataFrame  =
    {
        if(leftOn.size != rightOn.size)
        {
            println("leftOn and rightOn have different sizes")
            return  Seq.empty[(String)].toDF("error")
        }

        val dataColumns1 = df1.columns.toSeq
        val dataColumns2 = df2.columns.toSeq

        val newDataColumns1  = dataColumns1.map(x => x + sufixes(0))
        val newDataColumns2  = dataColumns2.map(x => x + sufixes(1))

        val mapDataColumns1 = (dataColumns1 zip newDataColumns1).toMap
        val dfRenamed1 = renameColumnsMap(df1, mapDataColumns1)

        val mapDataColumns2 = (dataColumns2 zip newDataColumns2).toMap
        val dfRenamed2 = renameColumnsMap(df2, mapDataColumns2)

        val newLeftOn  = leftOn.map(x => x + sufixes(0))
        val newRightOn  = rightOn.map(x => x + sufixes(1))

        var cond = dfRenamed1(newLeftOn(0)) === dfRenamed2(newRightOn(0))
        for(i <- 1 until leftOn.size)
        {
            val condTmp = dfRenamed1(newLeftOn(i)) === dfRenamed2(newRightOn(i))
            cond = cond && condTmp
        }

        val dfJoin = dfRenamed1.join(dfRenamed2, cond, how)

        var condLeft = dfRenamed2(newRightOn(0)).isNull
        var condRight = dfRenamed1(newLeftOn(0)).isNull
        for(i <- 1 until leftOn.size)
        {
            val condTmp1 = dfRenamed2(newRightOn(i)).isNull
            condLeft = condLeft && condTmp1

            val condTmp2 = dfRenamed1(newLeftOn(i)).isNull
            condRight = condRight && condTmp2
        }

        var dfJoinMerged = dfJoin.withColumn("_merge", when(condLeft, "left_only").otherwise(when(condRight, "right_only").otherwise("both")))

        val cond1 = dfJoinMerged("_merge") === "left_only"
        val cond2 = dfJoinMerged("_merge") === "both"
        val condf = cond1 || cond2

        var dropList = new ListBuffer[String]()
        for(i <- 0 until leftOn.size)
        {
            if(leftOn(i) == rightOn(i))
            {
                dfJoinMerged = dfJoinMerged.withColumn(leftOn(i), when(condf, dfJoinMerged(newLeftOn(i))).otherwise(dfJoinMerged(newRightOn(i))))
                dropList += newLeftOn(i)
                dropList += newRightOn(i)
            }
        }

        val dfJoinRenamed = dropColumns(dfJoinMerged, dropList.toSeq)

        if(indicator == false)
            return dfJoinRenamed.drop("_merge")

        return dfJoinRenamed
    }

    /**
    * concatAxis
    *
    * This method concatenates two data frames at rows or columns
    *
    * @param df1 Data Frame 1 to be concatenated
    * @param df2 Data Frame 2 to be concatenated
    * @param axis {0/’index’, 1/’columns’}, Default 0
    * @return a Data Frame concatenated
    */
    def concatAxis(df1: DataFrame, df2: DataFrame, axis: Integer = 0) : DataFrame =
    {
        if(axis == 0)
        {
            return df1.union(df2)
        }
        else if(axis == 1)
        {
            val dfLeft = df1.withColumn("idLeft", monotonicallyIncreasingId)
            val dfRight = df2.withColumn("idRight", monotonicallyIncreasingId)

            val cond = dfLeft("idLeft") === dfRight("idRight")
            val dfJoin = dfLeft.join(dfRight, cond, "outer")

            val dfResult = dropColumns(dfJoin, Seq("idLeft", "idRight"))

            return dfResult
        }
        else
        {
            return  Seq.empty[(String)].toDF("error")
        }
    }
}
