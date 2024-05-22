package com.example;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProductDTOCoder extends AtomicCoder<ProductDTO> {

    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

    public static ProductDTOCoder of() {
        return new ProductDTOCoder();
    }

    @Override
    public void encode(ProductDTO value, OutputStream outStream) throws CoderException, IOException {
        STRING_CODER.encode(value.toJson(), outStream);
    }

    @Override
    public ProductDTO decode(InputStream inStream) throws CoderException, IOException {
        return ProductDTO.fromJson(STRING_CODER.decode(inStream));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
        // This coder is deterministic
    }

    /**
     * Registers the coder with the specified registry.
     */
    public static void registerCoder(CoderRegistry registry) {
        registry.registerCoderForClass(ProductDTO.class, ProductDTOCoder.of());
    }
}
