package com.example.config;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ProductDataPipelineOptions extends DataflowPipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("input.json")
    String getInputFile();
    void setInputFile(String value);

    @Description("Akeneo pim host")
    String getHostUrl();
    void setHostUrl(String value);

    @Description("Akeneo client Id")
    String getClientId();
    void setClientId(String value);

    @Description("Akeneo secret")
    String getSecret();
    void setSecret(String value);

    @Description("username")
    String getUsername();
    void setUsername(String value);

    @Description("password")
    String getPassword();
    void setPassword(String value);
}
