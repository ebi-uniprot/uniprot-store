package org.uniprot.store.indexer.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.core.io.ClassPathResource;

/**
 * @author lgonzales
 * @since 10/11/2020
 */
class PeekableResourceAwareItemReaderTest {

    private PeekableResourceAwareItemReader<String> reader;

    @BeforeEach
    void setup() {
        FlatFileItemReader<String> delegate = new FlatFileItemReader<>();
        delegate.setLineMapper(new PassThroughLineMapper());
        reader = new PeekableResourceAwareItemReader<>();
        reader.setDelegate(delegate);
        reader.setResource(new ClassPathResource("genecentric/peekTest.txt"));
        reader.open(new ExecutionContext());
    }

    @Test
    void testRead() throws Exception {
        assertEquals("a", reader.read());
        assertEquals("b", reader.read());
        assertEquals("c", reader.read());
        assertNull(reader.read());
    }

    @Test
    void testPeek() throws Exception {
        assertEquals("a", reader.peek());
        assertEquals("a", reader.read());
        assertEquals("b", reader.read());
        assertEquals("c", reader.peek());
        assertEquals("c", reader.read());
        assertNull(reader.peek());
        assertNull(reader.read());
    }

    @Test
    void testCloseAndOpenNoPeek() throws Exception {
        assertEquals("a", reader.read());
        ExecutionContext executionContext = new ExecutionContext();
        reader.update(executionContext);
        reader.close();
        reader.open(executionContext);
        assertEquals("b", reader.read());
    }

    @Test
    void testCloseAndOpenWithPeek() throws Exception {
        assertEquals("a", reader.read());
        assertEquals("b", reader.peek());
        ExecutionContext executionContext = new ExecutionContext();
        reader.update(executionContext);
        reader.close();
        reader.open(executionContext);
        assertEquals("b", reader.read());
    }

    @Test
    void testCloseAndOpenWithPeekAndRead() throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        assertEquals("a", reader.read());
        assertEquals("b", reader.peek());
        reader.update(executionContext);
        reader.close();
        reader.open(executionContext);
        assertEquals("b", reader.read());
        assertEquals("c", reader.peek());
        reader.update(executionContext);
        reader.close();
        reader.open(executionContext);
        assertEquals("c", reader.read());
    }

    @Test
    void testGetResourceFileName() throws Exception {
        assertEquals("peekTest.txt", reader.getResourceFileName());
    }

    @Test
    void testCanReadUsingSetResource() throws Exception {
        FlatFileItemReader<String> delegate = new FlatFileItemReader<>();
        delegate.setLineMapper(new PassThroughLineMapper());

        PeekableResourceAwareItemReader<String> peekReader =
                new PeekableResourceAwareItemReader<>();
        peekReader.setDelegate(delegate);
        peekReader.setResource(new ClassPathResource("genecentric/peekTest.txt"));
        peekReader.open(new ExecutionContext());
        assertEquals("a", peekReader.read());
    }
}
