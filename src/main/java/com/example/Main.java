package com.example;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.util.ShardedKey;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;


/**
 * Hello world!
 *
 */
public class Main
{
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final OkHttpClient client = new OkHttpClient();

    public static void parseProductsJson(String[] args) {
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
            c.output(content);
        }
    }

    static class ParseJsonToProductDTOFn extends DoFn<String, ProductDTO> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String jsonString = c.element();
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
        @ProcessElement
        public void processElement(@Element KV<String, Iterable<ProductDTO>> kv) throws Exception {
            List<ProductDTO> products = new ArrayList<>();
            kv.getValue().forEach(products::add);

            // Convert the list of ProductDTO to JSON
            String json = objectMapper.writeValueAsString(products);

            // Create the request body
            RequestBody body = RequestBody.create(json, MediaType.get("application/json; charset=utf-8"));

            // Create the HTTP POST request
            Request request = new Request.Builder()
                    .url("http://localhost:8080/products")
                    .post(body)
                    .build();

            // Send the request and handle the response
            client.newCall(request).enqueue(new Callback() {
                @Override
                public void onFailure(@NotNull Call call, @NotNull IOException e) {
                    e.printStackTrace();
                }

                @Override
                public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                    if (response.isSuccessful()) {
                        System.out.println("Response status code: " + response.code());
                        System.out.println("Response body: " + response.body());
                    } else {
                        System.err.println("Request failed with status code: " + response.code());
                        System.err.println("Response body: " + response.body());
                    }
                }
            });
        }
    }

    public static void main( String[] args ) {
        parseProductsJson(args);
    }

}
