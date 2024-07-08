package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;

class UniParcCrossReferenceToWrapperTest {

    private UniParcCrossReferenceToWrapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new UniParcCrossReferenceToWrapper();
    }

    @Test
    void testCallWithEmptyList() throws Exception {
        UniParcEntry entry = getUniParcEntry();
        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(entry);
        assertFalse(result.hasNext());
    }

    @Test
    void testCallWithSingleXref() throws Exception {
        UniParcCrossReference xref =
                createUniParcCrossReference(UniParcDatabase.SWISSPROT, "P12345");
        UniParcEntry entry = getUniParcEntry(xref);
        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(entry);

        assertTrue(result.hasNext());
        UniParcCrossReferenceWrapper wrapper = result.next();
        assertEquals("UPI0000000001-SWISSPROT-P12345", wrapper.getId());
        assertEquals(xref, wrapper.getUniParcCrossReference());
        assertFalse(result.hasNext());
    }

    @Test
    void testCallWithMultipleUniqueXrefs() throws Exception {
        UniParcCrossReference xref1 =
                createUniParcCrossReference(UniParcDatabase.SWISSPROT, "P12345");
        UniParcCrossReference xref2 = createUniParcCrossReference(UniParcDatabase.TREMBL, "A0A123");
        UniParcEntry entry = getUniParcEntry(xref1, xref2);

        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(entry);

        assertTrue(result.hasNext());
        UniParcCrossReferenceWrapper wrapper1 = result.next();
        assertEquals("UPI0000000001-SWISSPROT-P12345", wrapper1.getId());
        assertEquals(xref1, wrapper1.getUniParcCrossReference());

        assertTrue(result.hasNext());
        UniParcCrossReferenceWrapper wrapper2 = result.next();
        assertEquals("UPI0000000001-TREMBL-A0A123", wrapper2.getId());
        assertEquals(xref2, wrapper2.getUniParcCrossReference());

        assertFalse(result.hasNext());
    }

    @Test
    void testCallWithDuplicateXrefs() throws Exception {
        UniParcCrossReference xref1 =
                createUniParcCrossReference(UniParcDatabase.SWISSPROT, "P12345");
        UniParcCrossReference xref2 =
                createUniParcCrossReference(UniParcDatabase.SWISSPROT, "P12345");
        UniParcEntry entry = getUniParcEntry(xref1, xref2);

        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(entry);

        assertTrue(result.hasNext());
        UniParcCrossReferenceWrapper wrapper1 = result.next();
        assertEquals("UPI0000000001-SWISSPROT-P12345", wrapper1.getId());
        assertEquals(xref1, wrapper1.getUniParcCrossReference());

        assertTrue(result.hasNext());
        UniParcCrossReferenceWrapper wrapper2 = result.next();
        assertEquals("UPI0000000001-SWISSPROT-P12345-1", wrapper2.getId());
        assertEquals(xref2, wrapper2.getUniParcCrossReference());

        assertFalse(result.hasNext());
    }

    private UniParcEntry getUniParcEntry(UniParcCrossReference... xrefs) {
        return new UniParcEntryBuilder()
                .uniParcId("UPI0000000001")
                .uniParcCrossReferencesSet(List.of(xrefs))
                .build();
    }

    private UniParcCrossReference createUniParcCrossReference(UniParcDatabase database, String id) {
        return new UniParcCrossReferenceBuilder()
                .database(database)
                .id(id)
                .versionI(1)
                .version(1)
                .active(true)
                .created(LocalDate.now())
                .lastUpdated(LocalDate.now())
                .geneName("gene1")
                .proteinName("protein1")
                .build();
    }
}
