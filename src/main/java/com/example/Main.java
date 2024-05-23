package com.example;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.util.ShardedKey;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void parseProductsJson(String[] args) {
        logger.info("Starting parseProductsJson with args: {}", (Object) args);
        PipelineOptionsFactory.register(ProductDataPipeline.class);
        ProductDataPipeline options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProductDataPipeline.class);
        Pipeline pipeline = Pipeline.create(options);
        ProductDTOCoder.registerCoder(pipeline.getCoderRegistry());

        PCollection<ProductDTO> transformedProducts = pipeline.apply(FileIO.match().filepattern(options.getInputFile()))
                .apply(FileIO.readMatches())
                .apply("ReadEntireFile", ParDo.of(new ReadEntireFileFn()))
                .apply("ParseJsonToProductDTO", ParDo.of(new ParseJsonToProductDTOFn()));

        PCollection<KV<String, ProductDTO>> kvProducts = transformedProducts.apply("AssignKey", ParDo.of(new AssignKeyFn()));

        PCollection<KV<ShardedKey<String>, Iterable<ProductDTO>>> batchedProducts = kvProducts
                .apply("BatchProducts", GroupIntoBatches.<String, ProductDTO>ofSize(10)
                        .withMaxBufferingDuration(Duration.standardSeconds(30))
                        .withShardedKey());

        PCollection<KV<String, Iterable<ProductDTO>>> unshardedBatches = batchedProducts.apply("UnshardBatches", ParDo.of(new UnshardBatchesFn()));

        unshardedBatches.apply("CallAPI", ParDo.of(new CallApiFn()));

        pipeline.run().waitUntilFinish();
    }

    private static void updateDescription(ProductDTO productDTO) {
        // Update description field
        Map<String, List<AttributeValueDTO>> values = productDTO.getValues();
        if (values != null && values.containsKey("description")) {
            List<AttributeValueDTO> descriptionList = values.get("description");
            if (descriptionList != null && !descriptionList.isEmpty()) {
                for (AttributeValueDTO attributeValueDTO : descriptionList) {
                    if ("en_US".equals(attributeValueDTO.getLocale()) && "ecommerce".equals(attributeValueDTO.getScope())) {
                        attributeValueDTO.setData(" The B4100 is a Windows desktop printer ");
                    }
                }
            }
        }
    }

    static class ReadEntireFileFn extends DoFn<FileIO.ReadableFile, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            FileIO.ReadableFile file = c.element();
            String content = new String(Files.readAllBytes(Paths.get(file.getMetadata().resourceId().toString())), StandardCharsets.UTF_8);
            logger.debug("Read file content: {}", content);
            c.output(content);
        }
    }

    static class ParseJsonToProductDTOFn extends DoFn<String, ProductDTO> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String jsonString = c.element();
            logger.debug("Parsing JSON: {}", jsonString);
            JsonNode jsonArray = objectMapper.readTree(jsonString);

            if (jsonArray.isArray()) {
                for (JsonNode jsonObject : jsonArray) {
                    ProductDTO productDTO = objectMapper.treeToValue(jsonObject, ProductDTO.class);
                    c.output(productDTO);
                }
            } else {
                // Handle case where the input is not an array (optional)
                ProductDTO productDTO = objectMapper.treeToValue(jsonArray, ProductDTO.class);
                c.output(productDTO);
            }
        }
    }

    static class AssignKeyFn extends DoFn<ProductDTO, KV<String, ProductDTO>> {
        @ProcessElement
        public void processElement(@Element ProductDTO productDTO, OutputReceiver<KV<String, ProductDTO>> out) {
            logger.debug("Assigning key to ProductDTO: {}", productDTO);
            out.output(KV.of("key", productDTO));
        }
    }

    static class UnshardBatchesFn extends DoFn<KV<ShardedKey<String>, Iterable<ProductDTO>>, KV<String, Iterable<ProductDTO>>> {
        @ProcessElement
        public void processElement(@Element KV<ShardedKey<String>, Iterable<ProductDTO>> kv, OutputReceiver<KV<String, Iterable<ProductDTO>>> out) {
            out.output(KV.of(kv.getKey().getKey(), kv.getValue()));
        }
    }

    static class CallApiFn extends DoFn<KV<String, Iterable<ProductDTO>>, Void> {
        private static final OkHttpClient client = new OkHttpClient();

        @ProcessElement
        public void processElement(@Element KV<String, Iterable<ProductDTO>> kv) throws Exception {
            for(ProductDTO product: Objects.requireNonNull(kv.getValue())) {
                // Convert the list of ProductDTO to JSON
                String json = objectMapper.writeValueAsString(product);
                // Create the request body
                RequestBody body = RequestBody.create(json, MediaType.get("application/json"));

                // Create the HTTP POST request
                Request request = new Request.Builder()
                        .url("http://localhost:8080/products")
                        .post(body)
                        .build();

                // Send the request and handle the response
                client.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(@NotNull Call call, @NotNull IOException e) {
                        logger.error("API call failed", e);
                    }

                    @Override
                    public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                        if (response.isSuccessful()) {
                            logger.info("API call successful for {}: status code {}", product.getIdentifier(), response.code());
                        } else {
                            logger.error("API call failed for {}: status code {}", product.getIdentifier(), response.code());
                        }
                    }
                });
            };
        }
    }

    public static void main( String[] args ) {
        logger.info("Starting application with args: {}", (Object) args);
        parseProductsJson(args);
    }

}
