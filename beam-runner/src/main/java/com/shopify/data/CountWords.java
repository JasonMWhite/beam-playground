package com.shopify.data;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.hdfs.HDFSFileSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.util.Arrays;

public class CountWords
{
    private static final Logger LOG = LoggerFactory.getLogger(CountWords.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(MyOptions.class);
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create().as(MyOptions.class);
        LOG.info(options.toString());

        Pipeline p = Pipeline.create(options);

        HDFSFileSource<LongWritable, Text> source = HDFSFileSource.from(options.getInputPath(), TextInputFormat.class, LongWritable.class, Text.class);

        p.apply(Read.from(source))
                .apply(FlatMapElements.via((KV<LongWritable, Text> element) -> Arrays.asList(element.getValue().toString().split(" ")))
                        .withOutputType(new TypeDescriptor<String>() { }))
                .apply(MapElements.via((SerializableFunction<String, String>) String::toUpperCase)
                        .withOutputType(new TypeDescriptor<String>() { }))
                .apply(Count.perElement())
                .apply(MapElements.via((KV<String, Long> entry) -> entry.getKey() + ", " + entry.getValue())
                        .withOutputType(new TypeDescriptor<String>() { }))
                .apply(TextIO.Write.to(options.getOutputPath()));

        p.run();
    }
}
