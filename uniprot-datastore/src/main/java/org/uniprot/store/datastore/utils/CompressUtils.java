package org.uniprot.store.datastore.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.nixxcode.jvmbrotli.common.BrotliLoader;
import com.nixxcode.jvmbrotli.dec.BrotliDecoderChannel;
import com.nixxcode.jvmbrotli.enc.BrotliEncoderChannel;
import com.nixxcode.jvmbrotli.enc.Encoder;

/**
 * @author lgonzales
 * @since 15/09/2020
 */
public class CompressUtils {

    private CompressUtils() {}

    public static byte[] compress(byte[] in) throws IOException {
        if (BrotliLoader.isBrotliAvailable()) {
            return brotliCompress(in);
        } else {
            return gZipCompress(in);
        }
    }

    public static byte[] decompress(byte[] in) throws IOException {
        if (BrotliLoader.isBrotliAvailable()) {
            return brotliDecompress(in);
        } else {
            return gzipDecompress(in);
        }
    }

    static byte[] brotliCompress(byte[] in) throws IOException {
        Encoder.Parameters params = new Encoder.Parameters();
        try (ByteArrayOutputStream dst = new ByteArrayOutputStream()) {
            try (WritableByteChannel encoder =
                    new BrotliEncoderChannel(Channels.newChannel(dst), params)) {
                encoder.write(ByteBuffer.wrap(in));
            }
            return dst.toByteArray();
        }
    }

    static byte[] gZipCompress(byte[] in) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
                gzip.write(in);
                gzip.flush();
            }
            return out.toByteArray();
        }
    }

    static byte[] brotliDecompress(byte[] in) throws IOException {
        try (ReadableByteChannel src = Channels.newChannel(new ByteArrayInputStream(in));
                ReadableByteChannel decoder = new BrotliDecoderChannel(src);
                InputStream resultStream = Channels.newInputStream(decoder);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int read = resultStream.read();
            while (read > -1) { // -1 means EOF
                out.write(read);
                read = resultStream.read();
            }
            out.flush();
            return out.toByteArray();
        }
    }

    static byte[] gzipDecompress(byte[] in) throws IOException {
        try (ByteArrayInputStream inInput = new ByteArrayInputStream(in);
                GZIPInputStream gunzip = new GZIPInputStream(inInput);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int read = gunzip.read();
            while (read > -1) { // -1 means EOF
                out.write(read);
                read = gunzip.read();
            }
            out.flush();
            return out.toByteArray();
        }
    }
}
