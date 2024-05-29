package com.example.config;

public class PipelineConfig {
    private String hostUrl;
    private String clientId;
    private String secret;
    private String username;
    private String password;
    private String inputFilePath;
    private String productsEndpoint;

    public String getHostUrl() {
        return hostUrl;
    }

    public void setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getInputFilePath() {
        return inputFilePath;
    }

    public void setInputFilePath(String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }

    public String getProductsEndpoint() {
        return productsEndpoint;
    }

    public void setProductsEndpoint(String productsEndpoint) {
        this.productsEndpoint = productsEndpoint;
    }
}
