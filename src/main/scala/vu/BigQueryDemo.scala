package vu

import java.io.{BufferedReader, InputStreamReader, InputStream}

import bigquery4s.{BigQuery, JobId, ProjectId}
import com.google.api.services.bigquery._
import com.google.api.services.bigquery.model.{Job, JobConfiguration, JobConfigurationQuery}
import scala.collection.JavaConversions._

class BigQueryDemo(bq: BigQuery, projectId:String) {


  private def createFindJob(bq:BigQuery, projectId: String, query:String): JobId = {
    val job: Job = {
      val config = new JobConfiguration()
//      config.getQuery.setQuery(query)
      val queryConfig = new JobConfigurationQuery
//      queryConfig.setAllowLargeResults(true)
      config.setQuery(queryConfig)
      new Job().setConfiguration(config)
    }
    job.getConfiguration.getQuery.setQuery(query)

    val insert:Bigquery#Jobs#Insert = {
      bq.underlying.jobs.insert(projectId, job).setProjectId(projectId)
    }

    JobId(ProjectId(projectId), insert.execute().getJobReference.getJobId)
  }

}

object BigQueryDemo {

  val bq = new BigQuery()

  val projectId = "mysamplebqproject"

  def main(args: Array[String]) {
    val repo = new BigQueryDemo(bq, projectId)
    val sql = "SELECT *  FROM [publicdata:samples.wikipedia] limit 1"
    val jobId = bq.startQuery(projectId, sql).value
    val result = bq.underlying.jobs().getQueryResults(projectId, jobId)
    result.execute().getRows.toList.foreach(_.get)
//    val stream = result.executeAsInputStream()
//    val br    = new BufferedReader(new InputStreamReader(stream))
//    val items = Iterator.continually(br.readLine()).takeWhile(_ != null)

  }
}
