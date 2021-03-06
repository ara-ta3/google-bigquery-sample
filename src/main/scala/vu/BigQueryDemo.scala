package vu

import bigquery4s.BigQuery

object BigQueryDemo {

  val bq = BigQuery.fromServiceAccount(
    "foo@developer.gserviceaccount.com",
    "/path/to/bar.p12"
  )

  val projectId = "mysamplebqproject"

  def main(args: Array[String]) {
    val sql = "SELECT *  FROM [publicdata:samples.wikipedia] limit 10"
    val jobId = bq.startQuery(projectId, sql)
    val result = bq.await(jobId)
    bq.getRows(result).foreach(println)
  }

}

