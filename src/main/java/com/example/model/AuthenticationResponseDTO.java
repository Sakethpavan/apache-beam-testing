package com.example.model;

public class AuthenticationResponseDTO {
    private final String access_token;
    private final int expires_in;
    private final String token_type;
    private final String scope;
    private final String refresh_token;

    public AuthenticationResponseDTO(String access_token, int expires_in, String token_type, String scope, String refresh_token) {
        this.access_token = access_token;
        this.expires_in = expires_in;
        this.token_type = token_type;
        this.scope = scope;
        this.refresh_token = refresh_token;
    }

    public String getAccess_token() {
        return access_token;
    }

    public int getExpires_in() {
        return expires_in;
    }

    public String getToken_type() {
        return token_type;
    }

    public String getScope() {
        return scope;
    }

    public String getRefresh_token() {
        return refresh_token;
    }
}
