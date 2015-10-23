package vu.repository

import java.io.{InputStreamReader, BufferedReader}

import bigquery4s.BigQuery
import scala.io.Source

trait WikipediaRepository {
  def findAll:Seq[String]

}

class DemoRepository(bq: BigQuery, projectId:String) extends WikipediaRepository {

  def findAll: Seq[String] = {
    val sql = "SELECT *  FROM [publicdata:samples.wikipedia]"
    val jobId = bq.startQuery(projectId, sql).value
    Source.fromInputStream(bq.underlying.jobs().getQueryResults(projectId, jobId).executeAsInputStream()).toStream.map(_.toString)
  }

}

object DemoRepository {

  val bq = new BigQuery()

  val projectId = "mysamplebqproject"

  def main(args: Array[String]) {

    val repo = new DemoRepository(bq, projectId)
    repo.findAll.foreach(println)
  }
}
