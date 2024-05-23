package com.example.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ProductDTO {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonProperty(value = "uuid", required = false) // Example usage of @JsonProperty
    private String uuid;

    @JsonProperty(value = "identifier", required = false)
    private String identifier;

    @JsonProperty(value = "enabled", required = false)
    private boolean enabled = true;

    @JsonProperty(value = "family", required = false)
    private String family;

    @JsonProperty(value = "categories", required = false)
    private List<String> categories = new ArrayList<>();

    @JsonProperty(value = "groups", required = false)
    private List<String> groups = new ArrayList<>();

    @JsonProperty(value = "parent", required = false)
    private String parent;

    @JsonProperty("values")
    private Map<String, List<AttributeValueDTO>> values;

    @JsonProperty("associations")
    private Map<String, Map<String, List<String>>> associations;

    @JsonProperty("quantifiedAssociations")
    private Map<String, List<QuantifiedAssociationDTO>> quantifiedAssociations ;

    @JsonProperty("created")
    private String created;

    @JsonProperty("updated")
    private String updated;

    @JsonProperty("metadata")
    private Map<String, String> metadata;

    @JsonProperty("qualityScores")
    private Map<String, Object> qualityScores;

    @JsonProperty("completenesses")
    private List<Map<String, Object>> completenesses;

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public List<String> getCategories() {
        return categories;
    }

    public void setCategories(List<String> categories) {
        this.categories = categories;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }

    public Map<String, List<AttributeValueDTO>> getValues() {
        return values;
    }

    public void setValues(Map<String, List<AttributeValueDTO>> values) {
        this.values = values;
    }

    public Map<String, Map<String, List<String>>> getAssociations() {
        return associations;
    }

    public void setAssociations(Map<String, Map<String, List<String>>> associations) {
        this.associations = associations;
    }

    public Map<String, List<QuantifiedAssociationDTO>> getQuantifiedAssociations() {
        return quantifiedAssociations;
    }

    public void setQuantifiedAssociations(Map<String, List<QuantifiedAssociationDTO>> quantifiedAssociations) {
        this.quantifiedAssociations = quantifiedAssociations;
    }

    public String getCreated() {
        return created;
    }

    public void setCreated(String created) {
        this.created = created;
    }

    public String getUpdated() {
        return updated;
    }

    public void setUpdated(String updated) {
        this.updated = updated;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public Map<String, Object> getQualityScores() {
        return qualityScores;
    }

    public void setQualityScores(Map<String, Object> qualityScores) {
        this.qualityScores = qualityScores;
    }

    public List<Map<String, Object>> getCompletenesses() {
        return completenesses;
    }

    public void setCompletenesses(List<Map<String, Object>> completenesses) {
        this.completenesses = completenesses;
    }

    public String toJson() throws IOException {
        return MAPPER.writeValueAsString(this);
    }

    public static ProductDTO fromJson(String json) throws IOException {
        return MAPPER.readValue(json, ProductDTO.class);
    }
}

