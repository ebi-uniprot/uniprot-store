package org.uniprot.store.datastore.utils;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.nixxcode.jvmbrotli.common.BrotliLoader;

/**
 * @author lgonzales
 * @since 15/09/2020
 */
class CompressUtilsTest {

    @Test
    void testBrotli() throws IOException {
        if (BrotliLoader.isBrotliAvailable()) {
            String testInput = "{test:\"this is a test input aaa asdv asd vasdv asdasd vasdasd\"}";
            byte[] input = testInput.getBytes();
            byte[] compressed = CompressUtils.brotliCompress(input);
            byte[] result = CompressUtils.brotliDecompress(compressed);

            assertNotNull(input);
            assertNotNull(compressed);
            assertTrue(input.length > compressed.length);
            assertNotNull(result);
            assertArrayEquals(input, result);
        }
    }

    @Test
    void testGZip() throws IOException {
        String testInput =
                "{test:\"this is a test input aaa a asd vasdv aaa test asd qqq ddd aaa\","
                        + "other:\"other is a test input aaa a asd vasdv aaa test asd qqq ddd aaa\"}";
        byte[] input = testInput.getBytes();
        byte[] compressed = CompressUtils.gZipCompress(input);
        assertTrue(input.length > compressed.length);
        byte[] result = CompressUtils.gzipDecompress(compressed);

        assertNotNull(input);
        assertNotNull(compressed);
        assertNotNull(result);
        assertArrayEquals(input, result);
    }

    @Test
    void roundTripTest() throws IOException {
        String testInput = "{test:\"this is a test input aaa asdv asd vasdv asdasd vasdasd\"}";
        byte[] input = testInput.getBytes();
        byte[] compressed = CompressUtils.compress(input);
        byte[] result = CompressUtils.decompress(compressed);

        assertNotNull(input);
        assertNotNull(compressed);
        assertTrue(input.length > compressed.length);
        assertNotNull(result);
        assertArrayEquals(input, result);
    }
}
