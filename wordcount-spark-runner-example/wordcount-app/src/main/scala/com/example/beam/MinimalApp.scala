package com.example.beam

import java.lang.{Long => JLong}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values.{KV, TypeDescriptors}

object MinimalApp {
  def extractWords = new DoFn[String, String] {
    @ProcessElement
    def processElement(c: ProcessContext): Unit = {
      for (word <- c.element().split("[^\\p{L}]+")) yield {
        if (!word.isEmpty) {
          c.output(word)
        }
      }
    }
  }

  def formatResults = new SimpleFunction[KV[String, JLong], String] {
    override def apply(input: KV[String, JLong]): String = {
      input.getKey + ": " + input.getValue
    }
  }

  def main(args: Array[String]) {
    val options = PipelineOptionsFactory.create
    val pipeline = Pipeline.create(options)

    val input = TextIO.read.from("gs://apache-beam-samples/shakespeare/kinglear.txt")
    val output = TextIO.write.to("output/wordcount")

    pipeline.apply("ReadFiles", input)
      .apply("ExtractWords", ParDo.of(extractWords))
      .apply("CountWords", Count.perElement[String])
      .apply("FormatResults", MapElements.into(TypeDescriptors.strings)
        .via(formatResults)
      )
      .apply("WriteFiles", output)

    pipeline.run.waitUntilFinish
  }
}