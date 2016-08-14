package com.shopify.data;

import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Arrays;
import java.util.List;

public class CountWords
{
    private static final Logger LOG = LoggerFactory.getLogger(CountWords.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        LOG.info(options.toString());

        Pipeline p = Pipeline.create(options);

        final List<String> LINES = Arrays.asList(
                "To be, or not to be: that is the question: ",
                "Whether 'tis nobler in the mind to suffer ",
                "The slings and arrows of outrageous fortune, ",
                "Or to take arms against a sea of troubles, ");

        p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of())
                .apply(FlatMapElements.via((String text) -> Arrays.asList(text.split(" ")))
                        .withOutputType(new TypeDescriptor<String>() {
                        }))
                .apply(MapElements.via((SerializableFunction<String, String>) String::toUpperCase)
                        .withOutputType(new TypeDescriptor<String>() {
                        }))
                .apply(Count.perElement())
                .apply(MapElements.via((KV<String, Long> entry) -> entry.getKey() + ", " + entry.getValue())
                        .withOutputType(new TypeDescriptor<String>() {
                        }))
                .apply(TextIO.Write.to("/tmp/out.txt"));

        p.run();
    }
}
