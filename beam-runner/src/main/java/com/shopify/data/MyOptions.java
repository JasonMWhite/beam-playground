package com.shopify.data;

import org.apache.beam.runners.spark.SparkPipelineOptions;

interface MyOptions extends SparkPipelineOptions {
    String getInputPath();
    void setInputPath(String value);

    String getOutputPath();
    void setOutputPath(String value);
}
