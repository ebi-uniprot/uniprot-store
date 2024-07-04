package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;

import scala.Tuple2;

class UniParcCrossReferenceToWrapperTest {

    private UniParcCrossReferenceToWrapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new UniParcCrossReferenceToWrapper();
    }

    @Test
    void testCallWithEmptyList() throws Exception {
        Tuple2<String, List<UniParcCrossReference>> tuple =
                new Tuple2<>("UPI0000000001", new ArrayList<>());
        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(tuple);
        assertFalse(result.hasNext());
    }

    @Test
    void testCallWithSingleXref() throws Exception {
        UniParcCrossReference xref =
                createUniParcCrossReference(UniParcDatabase.SWISSPROT, "P12345");
        List<UniParcCrossReference> xrefList = List.of(xref);
        Tuple2<String, List<UniParcCrossReference>> tuple = new Tuple2<>("UPI0000000001", xrefList);

        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(tuple);

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
        List<UniParcCrossReference> xrefList = List.of(xref1, xref2);
        Tuple2<String, List<UniParcCrossReference>> tuple = new Tuple2<>("UPI0000000001", xrefList);

        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(tuple);

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
        List<UniParcCrossReference> xrefList = List.of(xref1, xref2);
        Tuple2<String, List<UniParcCrossReference>> tuple = new Tuple2<>("UPI0000000001", xrefList);

        Iterator<UniParcCrossReferenceWrapper> result = mapper.call(tuple);

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
