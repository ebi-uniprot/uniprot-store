package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;

class UniParcCrossReferenceMapperTest {

    private static final String UNIPARC_ID = "UPI123456789";

    @Test
    void canMapEmpty() throws Exception {
        UniParcEntry entry = getEntry(0);
        UniParcCrossReferenceMapper mapper = new UniParcCrossReferenceMapper(3);
        Iterator<UniParcCrossReferencePair> result = mapper.call(entry);
        assertNotNull(result);
        assertFalse(result.hasNext());
    }

    @Test
    void canMapOnePair() throws Exception {
        UniParcEntry entry = getEntry(2);
        UniParcCrossReferenceMapper mapper = new UniParcCrossReferenceMapper(3);
        Iterator<UniParcCrossReferencePair> result = mapper.call(entry);
        assertNotNull(result);
        UniParcCrossReferencePair xrefPair = result.next();
        validatePair(xrefPair, 0, List.of("P10000", "P10001"));
        assertFalse(result.hasNext());
    }

    @Test
    void canMapOnePairExact() throws Exception {
        UniParcEntry entry = getEntry(3);
        UniParcCrossReferenceMapper mapper = new UniParcCrossReferenceMapper(3);
        Iterator<UniParcCrossReferencePair> result = mapper.call(entry);
        assertNotNull(result);
        UniParcCrossReferencePair xrefPair = result.next();
        validatePair(xrefPair, 0, List.of("P10000", "P10001", "P10002"));
        assertFalse(result.hasNext());
    }

    @Test
    void canMapMultiplePairsExact() throws Exception {
        UniParcEntry entry = getEntry(6);
        UniParcCrossReferenceMapper mapper = new UniParcCrossReferenceMapper(3);
        Iterator<UniParcCrossReferencePair> result = mapper.call(entry);
        assertNotNull(result);
        UniParcCrossReferencePair xrefPair = result.next();
        validatePair(xrefPair, 0, List.of("P10000", "P10001", "P10002"));
        assertTrue(result.hasNext());

        xrefPair = result.next();
        validatePair(xrefPair, 1, List.of("P10003", "P10004", "P10005"));
        assertFalse(result.hasNext());
    }

    @Test
    void canMapMultiplePairs() throws Exception {
        UniParcEntry entry = getEntry(7);
        UniParcCrossReferenceMapper mapper = new UniParcCrossReferenceMapper(3);
        Iterator<UniParcCrossReferencePair> result = mapper.call(entry);
        assertNotNull(result);
        UniParcCrossReferencePair xrefPair = result.next();
        validatePair(xrefPair, 0, List.of("P10000", "P10001", "P10002"));
        assertTrue(result.hasNext());

        xrefPair = result.next();
        validatePair(xrefPair, 1, List.of("P10003", "P10004", "P10005"));
        assertTrue(result.hasNext());

        xrefPair = result.next();
        validatePair(xrefPair, 2, List.of("P10006"));
        assertFalse(result.hasNext());
    }

    private static void validatePair(
            UniParcCrossReferencePair xrefPair, int index, List<String> expectedIds) {
        assertEquals(UNIPARC_ID + "_" + index, xrefPair.getKey());
        List<String> returnedIds =
                xrefPair.getValue().stream().map(UniParcCrossReference::getId).toList();
        assertEquals(expectedIds, returnedIds);
    }

    private UniParcEntry getEntry(int xrefSize) {
        UniParcEntryBuilder builder = new UniParcEntryBuilder().uniParcId(UNIPARC_ID);
        for (int i = 0; i < xrefSize; i++) {
            builder.uniParcCrossReferencesAdd(getCrossRef(i));
        }
        return builder.build();
    }

    private UniParcCrossReference getCrossRef(int index) {
        return new UniParcCrossReferenceBuilder()
                .active(true)
                .id("P1000" + index)
                .database(UniParcDatabase.SWISSPROT)
                .build();
    }
}
