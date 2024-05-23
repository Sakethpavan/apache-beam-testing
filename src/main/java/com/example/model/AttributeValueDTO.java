package com.example.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;


public class AttributeValueDTO {

    @JsonProperty("locale")
    private String locale;

    @JsonProperty("scope")
    private String scope;

    @JsonProperty("data")
    private Object data;

    @JsonProperty("linked_data")
    private Object linkedData;

    @JsonProperty("attribute_type")
    private String attributeType;

    @JsonProperty("reference_data_name")
    private String referenceDataName;

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("_links")
    private Map<String, Map<String, String>> links;

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getLinkedData() {
        return linkedData;
    }

    public void setLinkedData(Object linkedData) {
        this.linkedData = linkedData;
    }

    public String getAttributeType() {
        return attributeType;
    }

    public void setAttributeType(String attributeType) {
        this.attributeType = attributeType;
    }

    public String getReferenceDataName() {
        return referenceDataName;
    }

    public void setReferenceDataName(String referenceDataName) {
        this.referenceDataName = referenceDataName;
    }

    public Map<String, Map<String, String>> getLinks() {
        return links;
    }

    public void setLinks(Map<String, Map<String, String>> links) {
        this.links = links;
    }
}