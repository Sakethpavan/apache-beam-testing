package com.example.service;

import com.example.model.AuthenticationResponseDTO;
import okhttp3.*;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.Gson;
import org.apache.http.HttpHeaders;

import java.io.IOException;
import java.util.Base64;
import java.util.Objects;

public class AuthService {

    private final String clientId;
    private final String secret;
    private final String tokenEndpoint;
    private final String username;
    private final String password;
    private final OkHttpClient client;

    private String accessToken;
    private String refreshToken;
    private Long expiresIn;

    public AuthService(
            String clientId,
            String secret,
            String tokenEndpoint,
            String username,
            String password
    ) {
        this.clientId = clientId;
        this.secret = secret;
        this.tokenEndpoint = tokenEndpoint;
        this.username = username;
        this.password = password;
        this.client = new OkHttpClient();
    }

    public String getAccessToken() throws Exception {
        if (this.accessToken == null) {
            updateAccessAndRefreshTokens();
        } else {
            if (System.currentTimeMillis() > this.expiresIn) {
                // Refresh the access token
                updateAccessTokenFromRefreshToken();
            }
        }
        return accessToken;
    }

    private void updateAccessAndRefreshTokens() {
        String authString = Base64.getEncoder().encodeToString((clientId + ":" + secret).getBytes());
        String requestBody = "{\"grant_type\":\"password\",\"username\":\"" + username + "\",\"password\":\"" + password + "\"}";

        Request request = new Request.Builder()
                .url(tokenEndpoint)
                .header("Authorization", "Basic " + authString)
                .post(RequestBody.create(requestBody, MediaType.get("application/json")))
                .build();

        // Send the request and handle the response
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                AuthenticationResponseDTO responseBody = new Gson().fromJson(Objects.requireNonNull(response.body()).string(), AuthenticationResponseDTO.class);
                if (responseBody != null) {
                    this.accessToken = responseBody.getAccess_token();
                    this.refreshToken = responseBody.getRefresh_token();
                    this.expiresIn = (responseBody.getExpires_in() * 1000L) + System.currentTimeMillis();
                }
            } else {
                throw new RuntimeException("Failed to obtain token: " + Objects.requireNonNull(response.body()).string());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain token", e);
        }
    }

    private void updateAccessTokenFromRefreshToken() throws Exception {
        String authString = Base64.getEncoder().encodeToString(
                (this.clientId + ":" + this.secret).getBytes()
        );
        // Prepare the request body for refreshing the access token
        String requestBody = "{\"refresh_token\":\"" + this.refreshToken + "\",\"grant_type\":\"refresh_token\"}";

        // Build the request
        Request request = new Request.Builder()
                .url(this.tokenEndpoint)
                .header(HttpHeaders.AUTHORIZATION, "Basic " + authString)
                .post(RequestBody.create(requestBody, MediaType.get("application/json")))
                .build();

        // Send the request and handle the response
        try (Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                AuthenticationResponseDTO responseBody = new Gson().fromJson(Objects.requireNonNull(response.body()).string(), AuthenticationResponseDTO.class);
                if (responseBody != null) {
                    this.accessToken = responseBody.getAccess_token();
                    this.refreshToken = responseBody.getRefresh_token();
                    this.expiresIn = (responseBody.getExpires_in() * 1000L) + System.currentTimeMillis();
                }
            } else {
                throw new RuntimeException("Failed to obtain token: " + Objects.requireNonNull(response.body()).string());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain token", e);
        }
    }
}
