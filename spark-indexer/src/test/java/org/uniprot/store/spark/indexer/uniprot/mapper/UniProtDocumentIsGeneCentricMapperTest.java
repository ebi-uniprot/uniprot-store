package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

class UniProtDocumentIsGeneCentricMapperTest {

    @Test
    void testCall_WithPresentGeneCentric() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();
        document.proteomes = Set.of("P1", "P2");
        boolean expectedGeneCentric = true;

        // Tuple with document and a present Optional value.
        Tuple2<UniProtDocument, Optional<Boolean>> input =
                new Tuple2<>(document, Optional.of(expectedGeneCentric));

        // Act: Call the method.
        UniProtDocument result = mapper.call(input);

        // Assert: Check if the result has the expected isGeneCentric value.
        assertTrue(result.isGeneCentric);
        assertSame(document.proteomes, document.proteomeCanonicals);
    }

    @Test
    void testCall_WithNotPresentGeneCentric() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();
        document.proteomes = Set.of("P1", "P2");
        boolean expectedGeneCentric = false;

        // Tuple with document and a present Optional value.
        Tuple2<UniProtDocument, Optional<Boolean>> input =
                new Tuple2<>(document, Optional.of(expectedGeneCentric));

        // Act: Call the method.
        UniProtDocument result = mapper.call(input);

        // Assert: Check if the result has the expected isGeneCentric value.
        assertFalse(result.isGeneCentric);
        assertEquals(0, result.proteomeCanonicals.size());
    }

    @Test
    void testCall_WithEmptyGeneCentric() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();
        document.proteomes = Set.of("P1", "P2");

        Tuple2<UniProtDocument, Optional<Boolean>> input = new Tuple2<>(document, Optional.empty());

        UniProtDocument result = mapper.call(input);

        assertFalse(result.isGeneCentric);
        assertEquals(0, result.proteomeCanonicals.size());
    }
}
