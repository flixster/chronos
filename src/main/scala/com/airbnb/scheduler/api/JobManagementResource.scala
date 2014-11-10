package com.airbnb.scheduler.api

import java.util.logging.{Level, Logger}
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.core.Response.Status
import scala.Array
import scala.collection.mutable.ListBuffer

import com.airbnb.scheduler.config.SchedulerConfiguration
import com.airbnb.scheduler.graph.JobGraph
import com.airbnb.scheduler.jobs._
import com.codahale.metrics.Histogram
import com.datastax.driver.core.Row
import com.datastax.driver.core.ColumnDefinitions
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.google.inject.Inject
import com.codahale.metrics.annotation.Timed
import org.joda.time.{DateTimeZone, DateTime}

/**
 * The REST API for managing jobs.
 * @author Florian Leibert (flo@leibert.de)
 */
//TODO(FL): Create a case class that removes epsilon from the dependent.
@Path(PathConstants.jobBasePath)
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class JobManagementResource @Inject()(val jobScheduler: JobScheduler,
                                      val jobGraph: JobGraph,
                                      val configuration: SchedulerConfiguration,
                                      val jobMetrics: JobMetrics) {

  private[this] val log = Logger.getLogger(getClass.getName)

  private val objectMapper = new ObjectMapper
  private val mod =  new SimpleModule("JobManagementResourceModule")

  mod.addSerializer(classOf[Histogram], new HistogramSerializer)
  mod.addSerializer(classOf[JobStatWrapper], new JobStatWrapperSerializer)
  objectMapper.registerModule(mod)

  @Path(PathConstants.jobPatternPath)
  @DELETE
  @Timed
  def delete(@PathParam("jobName") jobName: String): Response = {
    try {
      require(!jobGraph.lookupVertex(jobName).isEmpty, "Job '%s' not found".format(jobName))
      val job = jobGraph.lookupVertex(jobName).get
      val children = jobGraph.getChildren(jobName)
      if (children.nonEmpty) {
        job match {
          case j: DependencyBasedJob =>
            val parents = jobGraph.parentJobs(j)
            children.foreach {
              child =>
                val childJob = jobGraph.lookupVertex(child).get.asInstanceOf[DependencyBasedJob]
                val newParents = childJob.parents.filter{name => name != job.name} ++ j.parents
                val newChild = childJob.copy(parents = newParents)
                jobScheduler.replaceJob(childJob, newChild)
                parents.foreach {p =>
                  jobGraph.removeDependency(p.name, job.name)
                  jobGraph.addDependency(p.name, newChild.name)
                }
            }
          case j: ScheduleBasedJob =>
            children.foreach {
              child =>
                jobGraph.lookupVertex(child).get match {
                  case childJob: DependencyBasedJob =>
                    val newChild = new ScheduleBasedJob(
                      schedule = j.schedule,
                      scheduleTimeZone = j.scheduleTimeZone,
                      name = childJob.name,
                      command = childJob.command,
                      epsilon = childJob.epsilon,
                      successCount = childJob.successCount,
                      errorCount = childJob.errorCount,
                      executor = childJob.executor,
                      executorFlags = childJob.executorFlags,
                      retries = childJob.retries,
                      owner = childJob.owner,
                      lastError = childJob.lastError,
                      lastSuccess = childJob.lastSuccess,
                      async = childJob.async,
                      cpus = childJob.cpus,
                      disk = childJob.disk,
                      mem = childJob.mem,
                      disabled = childJob.disabled,
                      uris = childJob.uris,
                      highPriority = childJob.highPriority
                    )
                    jobScheduler.updateJob(childJob, newChild)
                  case _ =>
                }
            }
        }
      }
      jobScheduler.sendNotification(job, "[Chronos] - Your job '%s' was deleted!".format(jobName))

      jobScheduler.deregisterJob(job, persist = true)
      Response.noContent().build
    } catch {
      case ex: IllegalArgumentException => {
        log.log(Level.INFO, "Bad Request", ex)
        return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage)
          .build()
      }
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        return Response.serverError().build
      }
    }
  }

  @GET
  @Path(PathConstants.jobStatsPatternPath)
  def getStat(@PathParam("jobName") jobName: String) : Response = {
    try {
      require(!jobGraph.lookupVertex(jobName).isEmpty, "Job '%s' not found".format(jobName))

      val histoStats = jobMetrics.getJobHistogramStats(jobName)
      val jobStatsList: List[TaskStat] = jobScheduler.jobStats.getParsedJobStatsByName(jobName, 30)
      val jobStatsWrapper = new JobStatWrapper(jobStatsList, histoStats)

      val wrapperStr = objectMapper.writeValueAsString(jobStatsWrapper)
      Response.ok(wrapperStr).build()
    } catch {
      case ex: IllegalArgumentException => {
        log.log(Level.INFO, "Bad Request", ex)
        Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage)
          .build()
      }
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        Response.serverError().build
      }
    }
  }

  @Path(PathConstants.jobPatternPath)
  @PUT
  @Timed
  def trigger(@PathParam("jobName") jobName: String): Response = {
    try {
      require(!jobGraph.lookupVertex(jobName).isEmpty, "Job '%s' not found".format(jobName))
      val job = jobGraph.getJobForName(jobName).get
      log.info("Manually triggering job:" + jobName)
      jobScheduler.taskManager.enqueue(TaskUtils.getTaskId(job, DateTime.now(DateTimeZone.UTC), 0), job.highPriority)
      Response.noContent().build
    } catch {
      case ex: IllegalArgumentException => {
        log.log(Level.INFO, "Bad Request", ex)
        return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage)
          .build()
      }
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        return Response.serverError().build
      }
    }
  }

  @POST
  @Path(PathConstants.jobTaskProgressPath)
  def updateTaskProgress(@PathParam("jobName") jobName: String,
          @PathParam("taskId") taskId: String,
          taskStat: TaskStat) : Response = {
    try {
      require(!jobGraph.lookupVertex(jobName).isEmpty, "Job '%s' not found".format(jobName))
      require(TaskUtils.isValidVersion(taskId), "Invalid task id format %s".format(taskId))

      val numAdditionalProcessed = taskStat.numAdditionalElementsProcessed match {
        case Some(num: Int) => num
        case None => 0
      }
      require(numAdditionalProcessed > 0,
          "numAdditionalElementsProcessed (%d) is not positive".format(numAdditionalProcessed))

      jobScheduler.jobStats.updateTaskProgress(jobName, taskId, numAdditionalProcessed)
      Response.noContent().build
    } catch {
      case ex: IllegalArgumentException => {
        log.log(Level.INFO, "Bad Request", ex)
        Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage)
          .build()
      }
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        Response.serverError().build
      }
    }
  }

  @Path(PathConstants.allJobsPath)
  @GET
  @Timed
  def list(): Response = {
    try {
      val jobs = ListBuffer[BaseJob]()
      import scala.collection.JavaConversions._
      jobGraph.dag.vertexSet().map({
        job =>
          jobs += jobGraph.getJobForName(job).get
      })
      return Response.ok(jobs.toList).build
    } catch {
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR)
      }
    }
  }

  @GET
  @Path(PathConstants.jobSearchPath)
  @Timed
  def search(@QueryParam("name") name: String,
             @QueryParam("command") command: String,
             @QueryParam("any") any: String,
             @QueryParam("limit") limit: Integer,
             @QueryParam("offset") offset: Integer
             ) = {
    try {
      val jobs = ListBuffer[BaseJob]()
      import scala.collection.JavaConversions._
      jobGraph.dag.vertexSet().map({
        job =>
          jobs += jobGraph.getJobForName(job).get
      })

      val _limit: Integer = limit match {
        case x: Integer =>
          x
        case _ =>
          10
      }
      val _offset: Integer = offset match {
        case x: Integer =>
          x
        case _ =>
          0
      }

      val filteredJobs = jobs.filter {
        x =>
          var valid = true
          if (name != null && !name.isEmpty && !x.name.toLowerCase.contains(name.toLowerCase)) {
            valid = false
          }
          if (command != null && !command.isEmpty && !x.command.toLowerCase.contains(command.toLowerCase)) {
            valid = false
          }
          if (!valid && any != null && !any.isEmpty &&
            (x.name.toLowerCase.contains(any.toLowerCase) || x.command.toLowerCase.contains(any.toLowerCase))) {
            valid = true
          }
          // Maybe add some other query parameters?
          valid
      }.toList.slice(_offset, _offset + _limit)
      Response.ok(filteredJobs).build
    } catch {
      case ex: Exception => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR)
      }
    }
  }

}
