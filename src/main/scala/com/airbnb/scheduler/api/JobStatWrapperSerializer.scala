package com.airbnb.scheduler.api

import com.airbnb.scheduler.jobs.JobStatWrapper
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.SerializerProvider

import org.joda.time.{DateTime, Duration}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, PeriodFormat, PeriodFormatter, PeriodFormatterBuilder}

class JobStatWrapperSerializer extends JsonSerializer[JobStatWrapper] {
  def serialize(jobStat: JobStatWrapper, json: JsonGenerator, provider: SerializerProvider) {
    json.writeStartObject()

    json.writeFieldName("histogram")
    json.writeObject(jobStat.hist)

    val taskStats = jobStat.taskStats
    json.writeFieldName("taskStatHistory")
    json.writeStartArray()
    for (taskStat <- taskStats) {
      json.writeStartObject()

      json.writeFieldName("taskId")
      json.writeString(taskStat.taskId)

      json.writeFieldName("jobName")
      json.writeString(taskStat.jobName)

      json.writeFieldName("slaveId")
      json.writeString(taskStat.taskSlaveId)

      var fmt = DateTimeFormat.forPattern("MM/dd/yy HH:mm:ss")
      //always show start time
      json.writeFieldName("startTime")
      taskStat.taskStartTs match {
        case Some(dT: DateTime) => {
          json.writeString(fmt.print(dT))
        }
        case None => {
          json.writeString("N/A")
        }
      }
      //show either end time or currently running
      json.writeFieldName("endTime")
      taskStat.taskEndTs match {
        case Some(dT: DateTime) => {
          json.writeString(fmt.print(dT))
        }
        case None => {
          json.writeString("N/A")
        }
      }

      taskStat.taskDuration match {
        case Some(dur: Duration) => {
          val pFmt = new PeriodFormatterBuilder()
            .appendDays().appendSuffix("d")
            .appendHours().appendSuffix("h")
            .appendMinutes().appendSuffix("m")
            .appendSeconds().appendSuffix("s")
            .toFormatter()

            json.writeFieldName("duration")
            json.writeString(pFmt.print(dur.toPeriod()))
        }
        case None =>
      }

      json.writeFieldName("status")
      json.writeString(taskStat.taskStatus.toString())

      //only write elements processed, ignore numAdditionalElementsProcessed
      taskStat.numElementsProcessed match {
        case Some(num: Long) => {
          json.writeFieldName("numElementsProcessed")
          json.writeNumber(num)
        }
        case None => {
          json.writeFieldName("numElementsProcessed")
          json.writeString("N/A")
        }
      }

      json.writeEndObject()
    }
    json.writeEndArray()

    json.writeEndObject()
  }
}