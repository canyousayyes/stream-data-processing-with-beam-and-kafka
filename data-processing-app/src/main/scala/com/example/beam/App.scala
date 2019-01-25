package com.example.beam

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.{KafkaIO, KafkaRecord}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Instant
import org.slf4j.LoggerFactory

object App {
  type InputRecord = KafkaRecord[String, String]

  private final val LOG = LoggerFactory.getLogger(App.getClass)

  def printRecord = new DoFn[InputRecord, InputRecord] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      val input = c.element
      LOG.info(s"Record: ${input.getKV.getValue}")
      c.output(input)
    }
  }

  def main(args : Array[String]) {
    val bootstrapServers = "kafka1:9092,kafka2:9092"
    val topic = "app-ratings"

    val options = PipelineOptionsFactory.create
    val p = Pipeline.create(options)

    val input = KafkaIO.read[String, String]
      .withBootstrapServers(bootstrapServers)
      .withTopic(topic)
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializer(classOf[StringDeserializer])
      .withStartReadTime(Instant.parse("2019-01-01"))
      .withMaxNumRecords(10)

    p.apply("ReadInput", input)
      .apply(ParDo.of(printRecord))

    p.run.waitUntilFinish
  }
}
