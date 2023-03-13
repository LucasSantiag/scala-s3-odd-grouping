package com.vigil.functions

import com.vigil.KeyPair

object RDDHelpersFunctions {
  /**
   * Transform Integer Tuple in a string concatenated by \t (tsv delimiter)
   *
   * @param pair KeyPair (Int, Int)
   * @return String concatenated by delimiter
   */
  def toTsvFormat(pair: KeyPair) =
    s"${pair._1}\t${pair._2}"

    /**
   * Split the String based on CSV or TSV delimiters
   *
   * @param l line parameter to apply regex to split
   * @return Split line as an Array[String]
   */
  def splitByDelimiters(l: String): Array[String] =
    l.split("[,\t]")

  /**
   * Remove all lines that are fully not Integers
   *
   * @param l Array of element in the RDD line
   * @return Array of Array containing all lines with some integer value defined
   */
  def removeRandomStringHeaders(l: Array[String]): Array[Array[Option[Int]]] = {
    Array(l).map(_.map(_.toIntOption)).filter(!_.forall(_.isEmpty))
  }

  /**
   * Check for Integers Tuple and apply zero to any value Null
   *
   * @param l Array of element in the RDD line
   * @return Returns key and 0 if the value is null/empty and return key and value for filled values
   */
  def transformEmptyLineInZero(l: Array[Option[Int]]): (Int, Int) =
    l match {
      case Array(Some(key), Some(value)) => (key, value)
      case Array(Some(key)) => (key, 0)
    }
}
