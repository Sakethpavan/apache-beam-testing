package com.example.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ProductDataPipelineOptions extends org.apache.beam.sdk.options.PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("input.json")
    String getInputFile();
    void setInputFile(String value);

    @Description("Akeneo pim host")
//    @Default.String("https://pim-8f53d76134.trial.akeneo.cloud")
    String getHostUrl();
    void setHostUrl(String value);

    @Description("Akeneo client Id")
//    @Default.String("8_66ezeyfxjbgocsk0w4cgokw0k8wooogw44kogwccos4gswo40w")
    String getClientId();
    void setClientId(String value);

    @Description("Akeneo secret")
//    @Default.String("49x98j1wqtmosg0okcw48os0kg00o0swoco48o4kos84s8w8w0")
    String getSecret();
    void setSecret(String value);

    @Description("username")
//    @Default.String("postman_1986")
    String getUsername();
    void setUsername(String value);

    @Description("password")
//    @Default.String("755677568")
    String getPassword();
    void setPassword(String value);
}
