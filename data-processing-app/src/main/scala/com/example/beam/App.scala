package com.example.beam


import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import collection.JavaConverters._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.io.kafka.{KafkaIO, KafkaRecord}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import scalaj.http.Http
import play.api.libs.json._


object App {
  private final val LOG = LoggerFactory.getLogger(App.getClass)

  def printRecord = new DoFn[KafkaRecord[String, GenericRecord], Unit] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      println(c.element.getKV.getValue.get("email"))
    }
  }

  def getSchemaFromRegistry(registryUrl: String, subject: String): String = {
    val url = s"${registryUrl}/subjects/${subject}/versions/latest"
    val res = Http(url).asString.body
    val json = Json.parse(res)
    return (json \ "schema").get.as[String]
  }

  def getKafkaInput(bootstrapServers: String, topic: String, registryUrl: String): KafkaIO.Read[String, GenericRecord] = {
    val schema = new Schema.Parser().parse(getSchemaFromRegistry(registryUrl, s"${topic}-value"))
    val consumerConfig = Map[String, Object](
      (AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl)
    )
    return KafkaIO.read[String, GenericRecord]
      .withBootstrapServers(bootstrapServers)
      .withTopic(topic)
      .withKeyDeserializer(classOf[StringDeserializer])
      .withValueDeserializerAndCoder(classOf[GenericAvroDeserializer], AvroCoder.of(schema))
      .updateConsumerProperties(consumerConfig.asJava)
  }

  def main(args : Array[String]) {
    val bootstrapServers = "kafka1:9092,kafka2:9092"
    val registryUrl = "http://schema_registry:8081"

    val options = PipelineOptionsFactory.create
    val pipeline = Pipeline.create(options)

    val input = getKafkaInput(bootstrapServers, "app-users", registryUrl)
      .withStartReadTime(Instant.parse("2019-01-01"))
      .withMaxNumRecords(20)

    pipeline.apply("Read", input)
      .apply("Print", ParDo.of(printRecord))

    pipeline.run.waitUntilFinish
  }
}
