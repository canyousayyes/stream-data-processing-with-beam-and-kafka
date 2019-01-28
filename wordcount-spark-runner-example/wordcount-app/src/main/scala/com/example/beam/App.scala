package com.example.beam

import collection.JavaConverters._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms.{Count, FlatMapElements, MapElements}
import org.apache.beam.sdk.values.{KV, TypeDescriptors}
import org.slf4j.LoggerFactory

object App {
  private final val LOG = LoggerFactory.getLogger(App.getClass)

  trait AppOptions extends PipelineOptions {
    @Description("Input path")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    def getInputPath: String
    def setInputPath(value: String): Unit

    @Description("Output path")
    @Required
    def getOutputPath: String
    def setOutputPath(value: String): Unit
  }

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory
      .fromArgs(args.mkString(" "))
      .withValidation()
      .as(classOf[AppOptions])
    val pipeline = Pipeline.create(options)
    val input = TextIO.read.from(options.getInputPath)
    val output = TextIO.write.to(options.getOutputPath)

    LOG.info(s"Running wordcount example from ${input} to ${output} ...")
    pipeline.apply("ReadFiles", input)
      .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.strings)
        .via((word: String) => word.split("[^\\p{L}]+").toIterable.asJava)
      )
      .apply("CountWords", Count.perElement[String])
      .apply("FormatResults", MapElements.into(TypeDescriptors.strings)
        .via((input: KV[String, java.lang.Long]) => s"${input.getKey}: ${input.getValue}")
      )
      .apply("WriteFiles", output)

    pipeline.run.waitUntilFinish
    LOG.info("Done.")
  }
}