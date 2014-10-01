package com.airbnb.scheduler.jobs

import com.airbnb.scheduler.config.CassandraConfiguration
import com.google.inject.Inject

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.{DriverException, QueryValidationException, QueryExecutionException, NoHostAvailableException}
import com.datastax.driver.core.Row

import org.apache.mesos.Protos.{TaskState, TaskStatus}
import org.joda.time.DateTime

import java.util.concurrent.ConcurrentHashMap

import java.util.logging.{Level, Logger}
import scala.collection.JavaConverters._
import scala.collection.mutable.{Map, HashMap, ListBuffer}
import scala.Some

object CurrentState extends Enumeration {
  type CurrentState = Value
  val idle, queued, running = Value
}

class JobStats @Inject() (clusterBuilder: Option[Cluster.Builder], config: CassandraConfiguration) {

  protected val jobStates = new HashMap[String, CurrentState.Value]()

  val log = Logger.getLogger(getClass.getName)
  var _session: Option[Session] = None
  val statements  = new ConcurrentHashMap[String, PreparedStatement]().asScala

  def getJobState(jobName: String) : String = {
    /**
     * MIKE NOTE: currently everything stored in memory, look into moving
     * this to Cassandra. ZK is not an option cause serializers and
     * deserializers need to be written. Need a good solution, potentially
     * lots of writes and very few reads (only on failover)
     */
    val status = jobStates.get(jobName) match {
      case Some(s) =>
        s.toString()
      case _ =>
        CurrentState.idle.toString()
    }
    status
  }

  def updateJobState(jobName: String, state: CurrentState.Value) {
    log.info("Updating state for job (%s) to %s".format(jobName, state))
    jobStates.put(jobName, state)
  }

  def getSession: Option[Session] = {
    _session match {
      case Some(s) => Some(s)
      case None =>
        clusterBuilder match {
          case Some(c) =>
            try {
              val session = c.build.connect()
              session.execute(new SimpleStatement(
                s"CREATE KEYSPACE IF NOT EXISTS ${config.cassandraKeyspace()}" +
                  " WITH REPLICATION = { 'class' : 'SimpleStrategy'," +
                  " 'replication_factor' : 3 };"
              ))
              session.execute(new SimpleStatement(
                s"USE ${config.cassandraKeyspace()};"
              ))

              session.execute(new SimpleStatement(
                s"CREATE TABLE IF NOT EXISTS ${config.cassandraTable()}" +
                  """
                    |(
                    |   id             VARCHAR,
                    |   ts             TIMESTAMP,
                    |   job_name       VARCHAR,
                    |   job_owner      VARCHAR,
                    |   job_schedule   VARCHAR,
                    |   job_parents    SET<VARCHAR>,
                    |   task_state     VARCHAR,
                    |   slave_id       VARCHAR,
                    |   message        VARCHAR,
                    |   attempt        INT,
                    |   is_failure     BOOLEAN,
                    | PRIMARY KEY (job_name, id, ts))
                    | WITH bloom_filter_fp_chance=0.100000 AND
                    | compaction = {'class':'LeveledCompactionStrategy'}
                  """.stripMargin
              ))
              _session = Some(session)
              _session
            } catch {
              case e: DriverException =>
                log.log(Level.WARNING, "Caught exception when creating Cassandra JobStats session", e)
                None
            }
          case None => None
        }
    }
  }

  def resetSession() {
    statements.clear()
    _session match {
      case Some(session) =>
        session.close()
      case _ =>
    }
    _session = None
  }

  /**
   * Queries Cassandra table for past and current job statistics by jobName
   * and limits by numTasks. The result is sorted by the intended task
   * execution time (aka Chronos due time)
   * @param jobName
   * @param numTasks
   * @return list of cassandra rows
   */
  private def getRecentJobStatsByName(jobName: String, numTasks: Int): Option[List[Row]] = {
    var rowsListFinal: Option[List[Row]] = None
    try {
      getSession match {
        case Some(session: Session) =>
          /*
           * NOTE: increasing limit by 4 cause each tasks on average takes
           * 2-3 entries in the table
           */
          val numTasksLimit = numTasks * 4

          /*
           * Since the numTasksLimit does not change that much, it is fair to
           * cache the prepared queries
           */
          val query = s"SELECT * FROM ${config.cassandraTable()} WHERE job_name='${jobName}' ORDER BY id DESC LIMIT ${numTasksLimit};"
          val prepared = statements.getOrElseUpdate(query, {
            session.prepare(
              new SimpleStatement(query)
                .asInstanceOf[RegularStatement]
            )
          })
          val resultSet = session.execute(prepared.bind())
          val rowsList = resultSet.all().asScala.toList
          rowsListFinal = Some(rowsList)
        case None => rowsListFinal = None
      }
    } catch {
      case e: NoHostAvailableException =>
        resetSession()
        log.log(Level.WARNING, "No hosts were available, will retry next time.", e)
      case e: QueryExecutionException =>
        log.log(Level.WARNING,"Query execution failed:", e)
      case e: QueryValidationException =>
        log.log(Level.WARNING,"Query validation failed:", e)
    }
    rowsListFinal
  }

  /**
   * Compare function for TaskStat by most recent date.
   */
  private def recentDateCompareFnc(a: TaskStat, b: TaskStat): Boolean = {
    var compareAscDate = a.taskStartTs match {
      case Some(aTs: DateTime) => {
        b.taskStartTs  match {
          case Some(bTs: DateTime) => {
            aTs.compareTo(bTs) <= 0
          }
          case None => {
            false
          }
        }
      }
      case None => {
        true
      }
    }
    !compareAscDate
  }

  /**
   * Returns most recent tasks by jobName and returns only numTasks
   * @param jobName
   * @param numTasks
   * @return returns a list of past and currently running tasks,
   *         the first element is the most recent.
   */
  def getParsedJobStatsByName(jobName: String, numTasks: Int): List[TaskStat] = {
    val taskMap = Map[String, TaskStat]()

    val rowsListOpt = getRecentJobStatsByName(jobName, numTasks) match {
      case Some(rowsList) => {
        for (row <- rowsList) {
          //go through all the rows and construct a job history
          //group elements by task id

          /*
           * Maintain order, however should not refer to the order
           * returned from the DB. This order reflects when tasks are
           * due (intended execution time). It is more correct to sort by
           * start time (execution timestamp)
           */
          var cDefs = row.getColumnDefinitions();

          if (cDefs.contains("job_name") &&
              cDefs.contains("id") &&
              cDefs.contains("ts") &&
              cDefs.contains("task_state") &&
              cDefs.contains("slave_id") &&
              cDefs.contains("is_failure")) {
            var jobName = row.getString("job_name")
            var taskId = row.getString("id")
            var taskTimestamp = row.getDate("ts")
            var taskState = row.getString("task_state")
            var slaveId = row.getString("slave_id")
            var isFailure = row.getBool("is_failure")

            var taskStats = taskMap.getOrElseUpdate(taskId, new TaskStat(taskId, jobName, slaveId))

            if (TaskState.TASK_RUNNING.toString == taskState) {
              taskStats.setTaskStartTs(taskTimestamp)
              taskStats.setTaskStatus(ChronosTaskStatus.Running)
            } else if ((TaskState.TASK_FINISHED.toString() == taskState) ||
              (TaskState.TASK_FAILED.toString() == taskState) ||
              (TaskState.TASK_KILLED.toString() == taskState) ||
              (TaskState.TASK_LOST.toString() == taskState)) {

              //terminal state
              taskStats.setTaskEndTs(taskTimestamp)
              if (TaskState.TASK_FINISHED.toString() == taskState) {
                taskStats.setTaskStatus(ChronosTaskStatus.Success)
              } else {
                taskStats.setTaskStatus(ChronosTaskStatus.Fail)
              }
            }
          } else {
            log.info("Invalid row found in cassandra table for jobName=%s".format(jobName))
          }
        }
      }
      case None => {
        log.info("No row list found for jobName=%s".format(jobName))
      }
    }

    //sort here, see note above
    val taskStatList = taskMap.values.toList
    var sortedDescTaskStatList = taskStatList.sortWith(recentDateCompareFnc)

    //limit here
    sortedDescTaskStatList = sortedDescTaskStatList.slice(0, numTasks)
    sortedDescTaskStatList
  }

  def jobQueued(job: BaseJob, attempt: Int) {
    updateJobState(job.name, CurrentState.queued)
  }

  def jobStarted(job: BaseJob, taskStatus: TaskStatus, attempt: Int) {
    updateJobState(job.name, CurrentState.running)
    try {
      getSession match {
        case Some(session: Session) =>
          job match {
            case job: ScheduleBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable()} (id, ts, job_name, job_owner, job_schedule, task_state, slave_id, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              session.executeAsync(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                job.schedule,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer
              ))
            case job: DependencyBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable()} (id, ts, job_name, job_owner, job_parents, task_state, slave_id, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              val parentSet: java.util.Set[String] = job.parents.asJava
              session.executeAsync(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                parentSet,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer
              ))
          }
        case None =>
      }
    } catch {
      case e: NoHostAvailableException =>
        resetSession()
        log.log(Level.WARNING, "No hosts were available, will retry next time.", e)
      case e: QueryExecutionException =>
        log.log(Level.WARNING,"Query execution failed:", e)
      case e: QueryValidationException =>
        log.log(Level.WARNING,"Query validation failed:", e)
    }
  }
  def jobFinished(job: BaseJob, taskStatus: TaskStatus, attempt: Int) {
    updateJobState(job.name, CurrentState.idle)
    try {
      getSession match {
        case Some(session: Session) =>
          job match {
            case job: ScheduleBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable()} (id, ts, job_name, job_owner, job_schedule, task_state, slave_id, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              session.executeAsync(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                job.schedule,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer
              ))
            case job: DependencyBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable()} (id, ts, job_name, job_owner, job_parents, task_state, slave_id, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              val parentSet: java.util.Set[String] = job.parents.asJava
              session.execute(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                parentSet,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer
              ))
          }
        case None =>
      }
    } catch {
      case e: NoHostAvailableException =>
        resetSession()
        log.log(Level.WARNING, "No hosts were available, will retry next time.", e)
      case e: QueryExecutionException =>
        log.log(Level.WARNING,"Query execution failed:", e)
      case e: QueryValidationException =>
        log.log(Level.WARNING,"Query validation failed:", e)
    }
  }
  def jobFailed(job: BaseJob, taskStatus: TaskStatus, attempt: Int) {
    updateJobState(job.name, CurrentState.idle)
    try {
      getSession match {
        case Some(session: Session) =>
          job match {
            case job: ScheduleBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable()} (id, ts, job_name, job_owner, job_schedule, task_state, slave_id, attempt, message, is_failure) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              session.executeAsync(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                job.schedule,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer,
                taskStatus.getMessage
              ))
            case job: DependencyBasedJob =>
              val query =
                s"INSERT INTO ${config.cassandraTable} (id, ts, job_name, job_owner, job_parents, task_state, slave_id, attempt, message, is_failure) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, true) USING TTL ${config.cassandraTtl()}"
              val prepared = statements.getOrElseUpdate(query, {
                session.prepare(
                  new SimpleStatement(query).setConsistencyLevel(ConsistencyLevel.valueOf(config.cassandraConsistency())).asInstanceOf[RegularStatement]
                )
              })
              val parentSet: java.util.Set[String] = job.parents.asJava
              session.executeAsync(prepared.bind(
                taskStatus.getTaskId.getValue,
                new java.util.Date(),
                job.name,
                job.owner,
                parentSet,
                taskStatus.getState.toString,
                taskStatus.getSlaveId.getValue,
                attempt: java.lang.Integer,
                taskStatus.getMessage
              ))
          }
        case None =>
      }
    } catch {
      case e: NoHostAvailableException =>
        resetSession()
        log.log(Level.WARNING, "No hosts were available, will retry next time.", e)
      case e: QueryExecutionException =>
        log.log(Level.WARNING,"Query execution failed:", e)
      case e: QueryValidationException =>
        log.log(Level.WARNING,"Query validation failed:", e)
    }
  }
}
