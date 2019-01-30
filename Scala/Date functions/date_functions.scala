import  org.joda.time._
import  org.apache.spark.sql.functions._

// ---------- Imports --------------------------------------------------------
import scala.util.control.Exception.allCatch
// import org.apache.spark.sql.functions.col
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.functions.udf

// ---------- Object ---------------------------------------------------------
// ====================================================================
// === DateConversion: This class provides all functionality for        ===
// === converting between different datetypes on top of what Java/Scala ===
// === provides as basic functionality.                                 ===
// ====================================================================
object DateConversion extends java.io.Serializable {

  def undashedDate(dashedDate: String): String = {
    return dashedDate.replace("-", "")
  }

  def myDateAdd(in: String, add: Int): String = {
    val out = new LocalDate(in).plusDays(add).toString()
    return out
  }

  val addDaysUdf = udf((date : String, add: Int) => {
    myDateAdd(date, add)
  })

  // return Int, difference between 2 date-strings (to_date - from_date = output) ; can be negative
  def myDateDiff(date_from: String, date_to: String): Int = {
    val from_date = new LocalDate(date_from);
    val to_date = new LocalDate(date_to);
    return Days.daysBetween(from_date, to_date).getDays()

  }

  def dateStringToYmd(dashedDate: String): Int = {
    return undashedDate(dashedDate).toInt;
  }

  // note: input ymd MUST be in yyyymmdd form
  def ymdToDateString(ymd: Int): String = {
    val ymdString = ymd.toString;
    return ymdString.slice(0, 4) + "-" + ymdString.slice(4, 6) + "-" + ymdString.slice(6, 8)

  }

} // end class