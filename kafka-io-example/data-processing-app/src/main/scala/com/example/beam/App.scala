package com.example.beam

import java.lang.{Long => JLong}
import java.util.stream.Collectors

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.{KafkaIO, KafkaRecord}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.KV
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.{Duration, Instant}
import org.slf4j.LoggerFactory
import play.api.libs.json._

object App {
  type InputRecord = KafkaRecord[String, String]
  type MovieRating = KV[JLong, JLong]

  private final val LOG = LoggerFactory.getLogger(App.getClass)

  def printRecord = new DoFn[InputRecord, InputRecord] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      val input = c.element
      LOG.info(s"Record: ${input.getKV.getValue}")
      c.output(input)
    }
  }

  def printRecord2 = new DoFn[MovieRating, MovieRating] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      val input = c.element
      LOG.info(s"Record 2: ${input.getKey} = ${input.getValue}")
      c.output(input)
    }
  }

  def printRecord3 = new DoFn[KV[JLong, java.util.List[JLong]], KV[JLong, java.util.List[JLong]]] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      val input = c.element
      val list = c.element.getValue.stream
        .map[String](n => n.toString)
        .collect(Collectors.joining(", "))
      LOG.info(s"Record 3: ${input.getKey} = ${list}")
      c.output(input)
    }
  }

  def parseRecord = new DoFn[InputRecord, MovieRating] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      val json = Json.parse(c.element.getKV.getValue)
      val movieId = json \ "payload" \ "movie_id"
      val rating = json \ "payload" \ "rating"
      if (movieId.isDefined && rating.isDefined) {
        // TODO extract timestamp and use c.outputWithTimestamp
        c.output(KV.of(
          movieId.as[Long].asInstanceOf[JLong],
          rating.as[Long].asInstanceOf[JLong]
        ))
      }
    }
  }

  def main(args : Array[String]) {
    val bootstrapServers = "kafka1:9092,kafka2:9092"
    val topic = "app-ratings"
    val startReadTime = Instant.parse("1970-01-01")

    val options = PipelineOptionsFactory.create
    val p = Pipeline.create(options)

    val input = KafkaIO.read[String, String]
      .withBootstrapServers(bootstrapServers)
      .withTopic(topic)
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializer(classOf[StringDeserializer])
      .withStartReadTime(startReadTime)

    val pp = p.apply("ReadInput", input)
      .apply(ParDo.of(printRecord))
      .apply(ParDo.of(parseRecord))
      .apply(Window.into[MovieRating](
        FixedWindows.of(Duration.standardMinutes(5))
      ))
      .apply(ParDo.of(printRecord2))
      .apply(ApproximateQuantiles.perKey(4))
      .apply(ParDo.of(printRecord3))


      //.apply(GroupByKey[Long, Long])
      //.apply(Combine.perKey[Long, Long, Long](Sum.ofLongs()))

    p.run.waitUntilFinish
  }
}
