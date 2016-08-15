package com.shopify.data;

import org.apache.beam.sdk.options.PipelineOptions;

interface MyOptions extends PipelineOptions {
    String getInputPath();
    void setInputPath(String value);

    String getOutputPath();
    void setOutputPath(String value);
}
