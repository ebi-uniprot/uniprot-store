package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/06/2020
 */
class UniParcMapperTest {

    @Test
    void testValidEntry() throws Exception {
        UniParcMapper mapper = new UniParcMapper();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "P12345_HUMAN", UniProtKBEntryType.SWISSPROT)
                        .build();
        Optional<String> uniparcId = Optional.of("UPI1234567890");
        Tuple2<UniProtKBEntry, Optional<String>> tuple = new Tuple2<>(entry, uniparcId);
        UniProtKBEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals("P12345", result.getPrimaryAccession().getValue());
        assertEquals("P12345_HUMAN", result.getUniProtkbId().getValue());
        assertEquals("SWISSPROT", result.getEntryType().name());

        assertNotNull(result.getExtraAttributes());
        assertEquals(1, result.getExtraAttributes().size());
        assertEquals(
                "UPI1234567890",
                result.getExtraAttributes().get(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB));
    }

    @Test
    void testEmptyJoin() throws Exception {
        UniParcMapper mapper = new UniParcMapper();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "P12345_HUMAN", UniProtKBEntryType.SWISSPROT)
                        .build();
        Optional<String> uniparcId = Optional.empty();
        Tuple2<UniProtKBEntry, Optional<String>> tuple = new Tuple2<>(entry, uniparcId);
        UniProtKBEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals("P12345", entry.getPrimaryAccession().getValue());
        assertEquals("P12345_HUMAN", entry.getUniProtkbId().getValue());
        assertEquals("SWISSPROT", entry.getEntryType().name());

        assertNotNull(entry.getExtraAttributes());
        assertTrue(entry.getExtraAttributes().isEmpty());
    }
}
