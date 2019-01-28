package com.example.beam

import collection.JavaConverters._
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.{Count, FlatMapElements, MapElements}
import org.apache.beam.sdk.values.{KV, TypeDescriptors}

object MinimalApp {
  def main(args: Array[String]) {
    val options = PipelineOptionsFactory.create
    val pipeline = Pipeline.create(options)

    val input = TextIO.read.from("gs://apache-beam-samples/shakespeare/kinglear.txt")
    val output = TextIO.write.to("output/wordcount")

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
  }
}