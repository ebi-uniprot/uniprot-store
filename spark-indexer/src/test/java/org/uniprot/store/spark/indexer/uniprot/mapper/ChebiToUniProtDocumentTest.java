package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.uniprot.mapper.ChebiToUniProtDocument.*;

import java.util.*;

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
        Set<String> catalyticValues = new HashSet<>();
        catalyticValues.add("CHEBI:1");
        catalyticValues.add("CHEBI:2");
        catalyticValues.add("NotChebiId");

        doc.commentMap.put(CC_CATALYTIC_ACTIVITY, catalyticValues);
        doc.commentMap.put(CC_CATALYTIC_ACTIVITY_EXP, catalyticValues);

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder()
                        .id("1")
                        .inchiKey("inch1")
                        .relatedIdsAdd(relatedId1)
                        .name("name1")
                        .synonymsSet(List.of("Syn11_1", "Syn11_2"))
                        .build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").synonymsAdd("syn22_1").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .synonymsSet(List.of("Syn2_1", "Syn11_1"))
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(9, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));
        assertTrue(result.chebi.containsAll(Set.of("name1", "Syn11_1", "Syn11_2", "Syn2_1")));

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

        assertEquals(resultCatalytic, result.commentMap.get(CC_CATALYTIC_ACTIVITY_EXP));
    }

    @Test
    void canMapCofactorChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        doc.cofactorChebi.add("CHEBI:1");
        doc.cofactorChebi.add("CHEBI:2");
        doc.cofactorChebi.add("NotChebiId");

        doc.commentMap.put(CC_COFACTOR_CHEBI_EXP, doc.cofactorChebi);

        ChebiEntry relatedId1 = new ChebiEntryBuilder().id("11").inchiKey("inch11").build();
        ChebiEntry chebi1 =
                new ChebiEntryBuilder()
                        .id("1")
                        .inchiKey("inch1")
                        .relatedIdsAdd(relatedId1)
                        .name("name1")
                        .synonymsAdd("synonym")
                        .build();

        ChebiEntry relatedId2 = new ChebiEntryBuilder().id("21").inchiKey("inch21").build();
        ChebiEntry relatedId3 = new ChebiEntryBuilder().id("22").build();
        ChebiEntry chebi2 =
                new ChebiEntryBuilder()
                        .id("2")
                        .inchiKey("inch2")
                        .name("name2")
                        .relatedIdsAdd(relatedId2)
                        .relatedIdsAdd(relatedId3)
                        .synonymsAdd("synonym")
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(8, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));
        assertTrue(result.chebi.containsAll(List.of("name1", "name2", "synonym")));

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

        assertEquals(doc.cofactorChebi, result.commentMap.get(CC_COFACTOR_CHEBI_EXP));
    }

    @Test
    void canMapCofactAndCatalyticToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        Set<String> catalyticValues = new HashSet<>();
        catalyticValues.add("CHEBI:1");
        catalyticValues.add("NotChebiId");

        doc.commentMap.put(CC_CATALYTIC_ACTIVITY, catalyticValues);
        doc.commentMap.put(CC_CATALYTIC_ACTIVITY_EXP, catalyticValues);

        doc.cofactorChebi.add("CHEBI:2");
        doc.commentMap.put(CC_COFACTOR_CHEBI_EXP, doc.cofactorChebi);

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
        assertEquals(resultCatalytic, result.commentMap.get(CC_CATALYTIC_ACTIVITY_EXP));

        assertEquals(5, result.cofactorChebi.size());
        assertTrue(result.cofactorChebi.contains("CHEBI:2"));
        assertTrue(result.cofactorChebi.contains("CHEBI:21"));
        assertTrue(result.cofactorChebi.contains("CHEBI:22"));
        assertTrue(result.cofactorChebi.contains("inch2"));
        assertTrue(result.cofactorChebi.contains("inch21"));
        assertEquals(doc.cofactorChebi, result.commentMap.get(CC_COFACTOR_CHEBI_EXP));
    }

    @Test
    void canMapBindingChebiToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        Set<String> bindingValues = new HashSet<>();
        bindingValues.add("CHEBI:1");
        bindingValues.add("CHEBI:2");
        bindingValues.add("Ca2+");
        bindingValues.add(null);

        doc.featuresMap.put(FT_BINDING, bindingValues);
        doc.featuresMap.put(FT_BINDING_EXP, bindingValues);

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
                        .synonymsAdd("synonym")
                        .build();

        Iterable<ChebiEntry> chebiEntries = List.of(chebi1, chebi2);
        Tuple2<UniProtDocument, Optional<Iterable<ChebiEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(chebiEntries));

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals(6, result.chebi.size());
        assertTrue(result.chebi.contains("CHEBI:1"));
        assertTrue(result.chebi.contains("CHEBI:2"));
        assertTrue(result.chebi.contains("CHEBI:11"));
        assertTrue(result.chebi.contains("CHEBI:21"));
        assertTrue(result.chebi.contains("CHEBI:22"));
        assertTrue(result.chebi.contains("synonym"));

        assertEquals(4, result.inchikey.size());
        assertTrue(result.inchikey.contains(chebi1.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId1.getInchiKey()));

        assertTrue(result.inchikey.contains(chebi2.getInchiKey()));
        assertTrue(result.inchikey.contains(relatedId2.getInchiKey()));

        Collection<String> resultBinding = result.featuresMap.get(FT_BINDING);
        assertNotNull(resultBinding);
        assertEquals(11, resultBinding.size());
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

        assertEquals(resultBinding, result.featuresMap.get(FT_BINDING_EXP));
    }

    @Test
    void canMapCatalyticAndCatalyticExperimentalToUniProtDocument() throws Exception {
        ChebiToUniProtDocument mapper = new ChebiToUniProtDocument();
        UniProtDocument doc = new UniProtDocument();
        Set<String> catalyticValues = new HashSet<>();
        catalyticValues.add("CHEBI:1");
        catalyticValues.add("CHEBI:2");
        catalyticValues.add("NotChebiId");

        Set<String> catalyticExpValues = new HashSet<>();
        catalyticExpValues.add("CHEBI:2");

        doc.commentMap.put(CC_CATALYTIC_ACTIVITY, catalyticValues);
        doc.commentMap.put(CC_CATALYTIC_ACTIVITY_EXP, catalyticExpValues);

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
        assertTrue(resultCatalytic.contains("CHEBI:11"));
        assertTrue(resultCatalytic.contains("inch1"));
        assertTrue(resultCatalytic.contains("inch11"));
        assertTrue(resultCatalytic.contains("CHEBI:2"));
        assertTrue(resultCatalytic.contains("CHEBI:21"));
        assertTrue(resultCatalytic.contains("CHEBI:22"));
        assertTrue(resultCatalytic.contains("inch2"));
        assertTrue(resultCatalytic.contains("inch21"));

        Collection<String> resultCatalyticExp = result.commentMap.get(CC_CATALYTIC_ACTIVITY_EXP);
        assertEquals(5, resultCatalyticExp.size());
        assertTrue(resultCatalyticExp.contains("CHEBI:2"));
        assertTrue(resultCatalyticExp.contains("CHEBI:21"));
        assertTrue(resultCatalyticExp.contains("CHEBI:22"));
        assertTrue(resultCatalyticExp.contains("inch2"));
        assertTrue(resultCatalyticExp.contains("inch21"));
    }
}
