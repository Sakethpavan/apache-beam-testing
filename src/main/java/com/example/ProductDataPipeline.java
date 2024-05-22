package com.example;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface ProductDataPipeline extends org.apache.beam.sdk.options.PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("input.json")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Default.String("output.json")
    String getOutputFile();
    void setOutputFile(String value);
}
