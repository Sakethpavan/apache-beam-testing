package com.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * Hello world!
 *
 */
public class Main
{
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void convertCsvToJson(String[] args){
        PipelineOptionsFactory.register(MyPipelineOptions.class);
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadCSV", TextIO.read().from(options.getInputFile()))
                .apply("ParseAndConvertToJSON", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String line, OutputReceiver<String> out) {
                        try {
                            // Assuming CSV format: id,name,price
                            String[] fields = line.split(",");
                            Product product = new Product(fields[0], fields[1], Integer.parseInt(fields[2]));
                            String json = objectMapper.writeValueAsString(product);
                            out.output(json);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }))
                .apply("WriteJSON", TextIO.write().to(options.getOutputFile()).withSuffix(".json").withoutSharding());

        pipeline.run().waitUntilFinish();
    }

    public static void parseProductsJson(String[] args) {
        PipelineOptionsFactory.register(ProductDataPipeline.class);
        ProductDataPipeline options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ProductDataPipeline.class);
        Pipeline pipeline = Pipeline.create(options);
        ProductDTOCoder.registerCoder(pipeline.getCoderRegistry());

        PCollection<String> input = pipeline.apply("ReadJSON", TextIO.read().from(options.getInputFile()));
        PCollection<ProductDTO> transformedProducts = input.apply("ParseProducts", ParDo.of(new DoFn<String, ProductDTO>() {
            @ProcessElement
            public void processElement(@Element String json, OutputReceiver<ProductDTO> out) {
                try {
                    // Deserialize JSON to ProductDTO object
                    ObjectMapper objectMapper = new ObjectMapper();
                    ProductDTO productDTO = objectMapper.readValue(json, ProductDTO.class);

                    // Update description field
                    updateDescription(productDTO);

                    // Output ProductDTO
                    out.output(productDTO);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
        transformedProducts.apply("CallAPI", ParDo.of(new DoFn<ProductDTO, Void>() {
            @ProcessElement
            public void processElement(@Element ProductDTO productDTO) {
                // Call API with ProductDTO
                // Implement API call logic here
                // Example:
                // apiClient.createProduct(productDTO);
                System.out.println("product DTO {}");
            }
        }));
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

    public static void main( String[] args ) {
        convertCsvToJson(args);
//        parseProductsJson(args);
    }

}
