package com.lib.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object DataFrameUtils 
{
    val spark = SparkSession.builder.appName("Data Frame Utils").getOrCreate()

    import spark.implicits._

    /**
    * dropColumns
    *
    * This method drop columns of Data Frame using Array.
    *
    * @param df Data Frame to be drop columns
    * @param columns Array with name columns
    * @return a Data Frame with columns dropped
    */
    def dropColumns(df: DataFrame, columns: Array[String]) : DataFrame = 
    {
        return df.drop(columns: _*)
    }

    /**
    * renameColumnsArray
    *
    * This method rename columns of data frame using Array.
    * The Array with the new names must obey the order of the columns that will be replaced
    *
    * @param df Data Frame to be renamed
    * @param columns Array with new name columns
    * @return a Data Frame with columns renamed
    */
    def renameColumnsArray(df: DataFrame, columns: Array[String]) : DataFrame = 
    {
        return df.toDF(columns: _*)
    }

    /**
    * renameColumnsMap
    *
    * This method rename columns of data frame using map.
    * if you rename all the columns use the renameColumnsArray
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
    * @param dfLeft Data Frame left to be merged
    * @param dfRight Data Frame right to be merged
    * @param leftOn Keys of left Data Frame
    * @param rightOn Keys of right Data Frame
    * @param complexOn Conditions that are not exclusively about equality between keys. 
                       The condition is expected to be ready and when this parameter 
                       is filled no name processing is performed, so it is necessary to treat the field names.
    * @param how Type of merge (Default: "outer")
    * @param suffixes List of sufixes for left and right Data Frame (Default: ["_x", "_y"])
    * @param indicator Adds a column to output Data Frame called “_merge” with information on the source of each row.
    *                  Information column is Categorical-type and takes on a value of “left_only” 
    *                  for observations whose merge key only appears in ‘left’ Data Frame, 
    *                  “right_only” for observations whose merge key only appears in ‘right’ Data Frame, 
    *                  and “both” if the observation’s merge key is found in both
    * @param allSuffixes A flag used to define whether suffixes in all columns or only in their 
                         equal columns is a feature that can not occur as keys
    * @return a Data Frame merged
    */
    /**  
    * TODO
    * Tratar as condicoes complexas, ou seja, criar um analisador de condicoes
    * para realizar tratamentos de sufixos para colunas iguais
    */  
    def merge(dfLeft: DataFrame, 
              dfRight: DataFrame, 
              leftOn: Array[String] = Array(), 
              rightOn: Array[String] = Array(), 
              complexOn: Column = col("null"), 
              how: String = "outer", 
              suffixes: Array[String] = Array("_x","_y"), 
              indicator: Boolean = false, 
              allSuffixes: Boolean = false) : DataFrame  =
    {
        if(leftOn.size != rightOn.size)
            throw new IllegalArgumentException("leftOn and rightOn have different sizes")

        if(complexOn != col("null"))
            return dfLeft.join(dfRight, complexOn, how)

        val dataColumns1 = dfLeft.columns.toArray
        val dataColumns2 = dfRight.columns.toArray

        val (dfRenamed1, dfRenamed2, newLeftOn, newRightOn) = if(allSuffixes)
        {
            val newDataColumns1 = dataColumns1.map(x => x + suffixes(0))
            val newDataColumns2 = dataColumns2.map(x => x + suffixes(1))

            val mapDataColumns1 = (dataColumns1 zip newDataColumns1).toMap
            val dfRenamed1 = renameColumnsMap(dfLeft, mapDataColumns1)

            val mapDataColumns2 = (dataColumns2 zip newDataColumns2).toMap
            val dfRenamed2 = renameColumnsMap(dfRight, mapDataColumns2)

            val newLeftOn = leftOn.map(x => x + suffixes(0))
            val newRightOn = rightOn.map(x => x + suffixes(1))

            (dfRenamed1, dfRenamed2, newLeftOn, newRightOn)
        }
        else
        {
            val equalColumns = dataColumns1.intersect(dataColumns2)
            val newLeftColumns = dataColumns1.map(x => if(equalColumns.contains(x)) x + suffixes(0) else x)
            val newRightColumns = dataColumns2.map(x => if(equalColumns.contains(x)) x + suffixes(1) else x)

            val dfRenamed1 = renameColumnsArray(dfLeft, newLeftColumns)
            val dfRenamed2 = renameColumnsArray(dfRight, newRightColumns)

            val newLeftOn = leftOn.map(x => if(equalColumns.contains(x)) x + suffixes(0) else x)
            val newRightOn = rightOn.map(x => if(equalColumns.contains(x)) x + suffixes(1) else x)

            (dfRenamed1, dfRenamed2, newLeftOn, newRightOn)
        }
        
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

        var dropList = new ArrayBuffer[String]()
        for(i <- 0 until leftOn.size)
        {
            if(leftOn(i) == rightOn(i))
            {
                dfJoinMerged = dfJoinMerged.withColumn(leftOn(i), when(condf, dfJoinMerged(newLeftOn(i))).otherwise(dfJoinMerged(newRightOn(i))))
                dropList += newLeftOn(i)
                dropList += newRightOn(i)
            }
        }

        val dfJoinRenamed = dropColumns(dfJoinMerged, dropList.toArray)

        if(indicator == false)
            return dfJoinRenamed.drop("_merge")

        return dfJoinRenamed
    }

    /**
    * concatAxis
    *
    * This method concatenates two data frames at rows or columns
    *
    * @param dfLeft Data Frame left to be concatenated
    * @param dfRight Data Frame right to be concatenated
    * @param axis {0/’index’, 1/’columns’}, Default 0
    * @return a Data Frame concatenated
    */
    def concatAxis(dfLeft: DataFrame, dfRight: DataFrame, axis: Integer = 0) : DataFrame =
    {
        if(axis == 0)
        {
            val colummsLeft = dfLeft.columns.toSet
            val columnsRight = dfRight.columns.toSet
            val totalColumns = colummsLeft ++ columnsRight

            val selectColumnsLeft = totalColumns.toList.map(x => if(colummsLeft.contains(x)) col(x) else lit(null).as(x))
            val selectColumnsRight = totalColumns.toList.map(x => if(columnsRight.contains(x)) col(x) else lit(null).as(x))

            val dfSelectedLeft = dfLeft.select(selectColumnsLeft:_*)
            val dfSelectedRight = dfRight.select(selectColumnsRight:_*)

            return dfSelectedLeft.union(dfSelectedRight)
        }
        else if(axis == 1)
        {
            val dfLeftId = dfLeft.withColumn("idLeft", monotonicallyIncreasingId)
            val dfRightId = dfRight.withColumn("idRight", monotonicallyIncreasingId)

            val dfJoin = merge(dfLeftId, dfRightId, leftOn=Array("idLeft"), rightOn=Array("idRight"))

            val dfResult = dropColumns(dfJoin, Array("idLeft", "idRight"))

            return dfResult
        }
        else
        {
            throw new IllegalArgumentException("Axis not found. Should be 0 or 1")
        }
    }
}