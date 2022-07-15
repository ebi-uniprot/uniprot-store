package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.uniprot.mapper.ChebiToUniProtDocument.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

class ChebiToUniProtDocumentTest {

    @Test
    void canMapCatalyticChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<String> catalyticValues = new ArrayList<>();
        catalyticValues.add("CHEBI:1");
        catalyticValues.add("CHEBI:2");
        catalyticValues.add("NotChebiId");

        doc.commentMap.put(CC_CATALYTIC_ACTIVITY, catalyticValues);

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder().id("1").inchiKey("inch1").relatedIdsAdd(relatedId1).build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(5, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));

        assertEquals(4, result.inchikey.size());
        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId1.getInchiKey()));

        assertTrue(result.inchikey.contains(chebi2.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId2.getInchiKey()));

        Collection<String> resultCatalytic = result.commentMap.get(CC_CATALYTIC_ACTIVITY);
        assertNotNull(resultCatalytic);
        assertEquals(10, resultCatalytic.size());
        assertTrue(resultCatalytic.contains("NotChebiId"));
        assertTrue(resultCatalytic.contains("CHEBI:1"));
        assertTrue(resultCatalytic.contains("inch1"));
        assertTrue(resultCatalytic.contains("CHEBI:11"));
        assertTrue(resultCatalytic.contains("inch11"));

        assertTrue(resultCatalytic.contains("CHEBI:2"));
        assertTrue(resultCatalytic.contains("inch2"));
        assertTrue(resultCatalytic.contains("CHEBI:21"));
        assertTrue(resultCatalytic.contains("inch21"));
        assertTrue(resultCatalytic.contains("CHEBI:22"));
    }

    @Test
    void canMapCofactorChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        doc.cofactorChebi.add("CHEBI:1");
        doc.cofactorChebi.add("CHEBI:2");
        doc.cofactorChebi.add("NotChebiId");

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder().id("1").inchiKey("inch1").relatedIdsAdd(relatedId1).build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(5, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));

        assertEquals(4, result.inchikey.size());
        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId1.getInchiKey()));

        assertTrue(result.inchikey.contains(chebi2.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId2.getInchiKey()));

        assertNotNull(doc.cofactorChebi);
        assertEquals(10, doc.cofactorChebi.size());
        assertTrue(doc.cofactorChebi.contains("NotChebiId"));
        assertTrue(doc.cofactorChebi.contains("CHEBI:1"));
        assertTrue(doc.cofactorChebi.contains("inch1"));
        assertTrue(doc.cofactorChebi.contains("CHEBI:11"));
        assertTrue(doc.cofactorChebi.contains("inch11"));

        assertTrue(doc.cofactorChebi.contains("CHEBI:2"));
        assertTrue(doc.cofactorChebi.contains("inch2"));
        assertTrue(doc.cofactorChebi.contains("CHEBI:21"));
        assertTrue(doc.cofactorChebi.contains("inch21"));
        assertTrue(doc.cofactorChebi.contains("CHEBI:22"));
    }

    @Test
    void canMapCofactAndCatalyticToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<String> catalyticValues = new ArrayList<>();
        catalyticValues.add("CHEBI:1");
        catalyticValues.add("NotChebiId");

        doc.commentMap.put(CC_CATALYTIC_ACTIVITY, catalyticValues);
        doc.cofactorChebi.add("CHEBI:2");

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder().id("1").inchiKey("inch1").relatedIdsAdd(relatedId1).build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .build();
        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(5, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));

        assertEquals(4, result.inchikey.size());
        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId1.getInchiKey()));
        assertTrue(result.inchikey.contains(chebi2.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId2.getInchiKey()));

        Collection<String> resultCatalytic = result.commentMap.get(CC_CATALYTIC_ACTIVITY);
        assertNotNull(resultCatalytic);
        assertEquals(5, resultCatalytic.size());
        assertTrue(resultCatalytic.contains("NotChebiId"));
        assertTrue(resultCatalytic.contains("CHEBI:1"));
        assertTrue(resultCatalytic.contains("CHEBI:11"));
        assertTrue(resultCatalytic.contains("inch1"));
        assertTrue(resultCatalytic.contains("inch11"));

        assertEquals(5, result.cofactorChebi.size());
        assertTrue(result.cofactorChebi.contains("CHEBI:2"));
        assertTrue(result.cofactorChebi.contains("CHEBI:21"));
        assertTrue(result.cofactorChebi.contains("CHEBI:22"));
        assertTrue(result.cofactorChebi.contains("inch2"));
        assertTrue(result.cofactorChebi.contains("inch21"));
    }

    @Test
    void canMapBindingChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        List<String> bindingValues = new ArrayList<>();
        bindingValues.add("CHEBI:1");
        bindingValues.add("CHEBI:2");
        bindingValues.add("Ca2+");

        doc.featuresMap.put(FT_BINDING, bindingValues);

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder().id("1").inchiKey("inch1").relatedIdsAdd(relatedId1).build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(5, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));

        assertEquals(4, result.inchikey.size());
        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId1.getInchiKey()));

        assertTrue(result.inchikey.contains(chebi2.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId2.getInchiKey()));

        Collection<String> resultBinding = result.featuresMap.get(FT_BINDING);
        assertNotNull(resultBinding);
        assertEquals(10, resultBinding.size());
        assertTrue(resultBinding.contains("Ca2+"));
        assertTrue(resultBinding.contains("CHEBI:1"));
        assertTrue(resultBinding.contains("inch1"));
        assertTrue(resultBinding.contains("CHEBI:11"));
        assertTrue(resultBinding.contains("inch11"));

        assertTrue(resultBinding.contains("CHEBI:2"));
        assertTrue(resultBinding.contains("inch2"));
        assertTrue(resultBinding.contains("CHEBI:21"));
        assertTrue(resultBinding.contains("inch21"));
        assertTrue(resultBinding.contains("CHEBI:22"));
    }
}
