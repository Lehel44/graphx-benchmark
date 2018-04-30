package util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object GraphUtils {

  def unionDifferentTables(df1: DataFrame, df2: DataFrame): DataFrame = {

    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val total = cols1 ++ cols2 // union

    val order = df1.columns ++  df2.columns
    val sorted = total.toList.sortWith((a,b)=> order.indexOf(a) < order.indexOf(b))

    def expr(myCols: Set[String], allCols: List[String]) = {
      allCols.map( {
        case x if myCols.contains(x) => col(x)
        case y => lit(null).as(y)
      })
    }

    df1.select(expr(cols1, sorted): _*).union(df2.select(expr(cols2, sorted): _*))
  }

}
