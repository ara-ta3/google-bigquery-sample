package vu


import bigquery4s.BigQuery
import org.joda.time.DateTime

object BigQueryDemo3 {
  val bq = BigQuery.fromServiceAccount(
    "foo@developer.gserviceaccount.com",
    "/path/to/bar.p12"
  )

  val projectId = "mysamplebqproject"

  def main(args: Array[String]) {
    val sql = "SELECT FORMAT_UTC_USEC(created) as a, TIMESTAMP_TO_MSEC(created) as b FROM [dummy.foo] LIMIT 10"
    val jobId = bq.startQuery(projectId, sql)
    val result = bq.await(jobId)
    bq.getRows(result).foreach(r => println(r.cells.head.value.getOrElse("")))
    bq.getRows(result).foreach(r => println(new DateTime(r.cells(1).value.getOrElse("").toString.toLong)))
  }

}
