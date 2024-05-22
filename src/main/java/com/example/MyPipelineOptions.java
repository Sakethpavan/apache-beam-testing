package com.example;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface MyPipelineOptions extends PipelineOptions {

    @Description("Path of the input file")
    @Default.String("input.csv")
    String getInputFile();
    void setInputFile(String value);

    @Description("Path of the output file")
    @Validation.Required
    String getOutputFile();
    void setOutputFile(String value);
}
