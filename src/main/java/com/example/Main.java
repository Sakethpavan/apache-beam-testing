package com.example;
import com.example.config.ProductDataPipelineOptions;
import com.example.model.ProductDTO;
import com.example.model.ProductDTOCoder;
import com.example.service.AuthService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import okhttp3.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

    public static Pipeline constructPipeline( String accessToken, ProductDataPipelineOptions options) {
        logger.info("creating pipeline");
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

        String akeneoProductsEndpoint = options.getHostUrl() + "/api/rest/v1/products";
        unshardedBatches.apply("CallAPI", ParDo.of(new CallApiFn(accessToken, akeneoProductsEndpoint)));

        return pipeline;
    }

    static class ReadEntireFileFn extends DoFn<FileIO.ReadableFile, String> {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            FileIO.ReadableFile file = c.element();
            String filePath = file.getMetadata().resourceId().toString();
            if (filePath.startsWith("gs:")) {
                // read data from storage bucket
                int lastIndex = filePath.lastIndexOf('/');
                String fullBucketName = filePath.substring(0, lastIndex);
                String bucketName = fullBucketName.split("gs://")[1];
                String objectName = filePath.substring(lastIndex + 1);
                Storage storage = StorageOptions.newBuilder()
                        .build()
                        .getService();
                Blob blob = storage.get(bucketName, objectName);
                String content = new String(blob.getContent());
                logger.debug("Read file content: {}", content);
                c.output(content);
            } else {
                // read local file
                String content = new String(Files.readAllBytes(Paths.get(file.getMetadata().resourceId().toString())), StandardCharsets.UTF_8);
                logger.debug("Read file content: {}", content);
                c.output(content);
            }
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
        private final String accessToken;
        private final String productsEndpoint;

        public CallApiFn(String accessToken, String productsEndpoint) {
            this.accessToken = accessToken;
            this.productsEndpoint = productsEndpoint;
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {
            for (ProductDTO product : Objects.requireNonNull(context.element().getValue())) {
                String json = objectMapper.writeValueAsString(product);
                RequestBody body = RequestBody.create(json, MediaType.get("application/json"));
                Request request = new Request.Builder()
                        .url(productsEndpoint)
                        .header("Authorization", "Bearer " + accessToken)
//                        .post(body)
                        .build();

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
            }
        }
    }

    public static void main( String[] args ) {
        logger.info("Starting application with args: {}", (Object) args);
        try {
            PipelineOptionsFactory.register(ProductDataPipelineOptions.class);
            ProductDataPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProductDataPipelineOptions.class);
            String tokenEndpoint = options.getHostUrl() + "/api/oauth/v1/token";
            AuthService authService = new AuthService(
                    options.getClientId(),
                    options.getSecret(),
                    tokenEndpoint,
                    options.getUsername(),
                    options.getPassword()
            );
            String accessToken = authService.getAccessToken();
            Pipeline pipeline = constructPipeline(accessToken, options);
            pipeline.run().waitUntilFinish();
        } catch (Exception e) {
            logger.error("Error occurred during pipeline execution", e);
        }
    }

}
