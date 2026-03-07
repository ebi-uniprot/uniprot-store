package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

class UniProtDocumentIsGeneCentricMapperTest {

    @Test
    void testCall_WithPresentGeneCentric() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();
        boolean expectedGeneCentric = true;

        // Tuple with document and a present Optional value.
        Tuple2<UniProtDocument, Optional<Boolean>> input =
                new Tuple2<>(document, Optional.of(expectedGeneCentric));

        // Act: Call the method.
        UniProtDocument result = mapper.call(input);

        // Assert: Check if the result has the expected isGeneCentric value.
        assertTrue(result.isGeneCentric);
    }

    @Test
    void testCall_WithEmptyGeneCentric() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();

        Tuple2<UniProtDocument, Optional<Boolean>> input = new Tuple2<>(document, Optional.empty());

        UniProtDocument result = mapper.call(input);

        assertFalse(result.isGeneCentric);
    }

    @Test
    void testCall_WithNullOptional() throws Exception {
        UniProtDocumentIsGeneCentricMapper mapper = new UniProtDocumentIsGeneCentricMapper();
        UniProtDocument document = new UniProtDocument();

        // Tuple with document and a null Optional.
        Tuple2<UniProtDocument, Optional<Boolean>> input = new Tuple2<>(document, null);

        UniProtDocument result = mapper.call(input);

        assertFalse(result.isGeneCentric);
    }
}
