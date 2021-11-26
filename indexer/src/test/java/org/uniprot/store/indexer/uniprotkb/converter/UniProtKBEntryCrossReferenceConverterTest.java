package org.uniprot.store.indexer.uniprotkb.converter;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.uniprot.cv.go.RelationshipType.IS_A;
import static org.uniprot.cv.go.RelationshipType.PART_OF;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.core.uniprotkb.xdb.UniProtKBDatabase;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;
import org.uniprot.cv.go.GORepo;
import org.uniprot.cv.xdb.UniProtKBDatabaseImpl;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtKBEntryCrossReferenceConverterTest {

    @Test
    void convertProteomeCrossReferences() {
        GORepo goRelationRepo = mock(GORepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("Proteomes"),
                        "id value",
                        new Property("Component", "PC12345"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(Arrays.asList("proteomes-id value", "id value")), document.crossRefs);

        assertEquals(Collections.singleton("proteomes"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_proteomes"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_proteomes"));

        assertEquals(Collections.singleton("id value"), document.proteomes);
        assertEquals(Collections.singleton("PC12345"), document.proteomeComponents);
        assertEquals(Collections.singleton("PC12345"), document.content);
    }

    @Test
    void convertGoCrossReferences() {
        GORepo goRelationRepo = mock(GORepo.class);
        when(goRelationRepo.getAncestors("GO:12345", asList(IS_A, PART_OF)))
                .thenReturn(
                        Collections.singleton(
                                new GeneOntologyEntryBuilder()
                                        .id("GO:0097440")
                                        .name("GOTERM")
                                        .build()));

        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("GO"),
                        "GO:12345",
                        new Property("GoTerm", "C:apical dendrite"),
                        new Property("GoEvidenceType", "IDA:UniProtKB"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(Set.of("apical dendrite"), document.content);

        assertEquals(new HashSet<>(Arrays.asList("go-GO:12345", "GO:12345")), document.crossRefs);

        assertEquals(Collections.singleton("go"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_go"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_go"));

        assertEquals(
                new HashSet<>(Arrays.asList("0097440", "12345", "GOTERM", "apical dendrite")),
                document.goes);
        assertEquals(new HashSet<>(Arrays.asList("0097440", "12345")), document.goIds);

        assertTrue(document.goWithEvidenceMaps.containsKey("go_ida"));
        assertEquals(
                new HashSet<>(Arrays.asList("0097440", "12345", "GOTERM", "apical dendrite")),
                document.goWithEvidenceMaps.get("go_ida"));
    }

    @Test
    void convertPDBCrossReferences() {
        GORepo goRelationRepo = mock(GORepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("PDB"),
                        "id value",
                        new Property("id", "PDB12345"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(new HashSet<>(Arrays.asList("pdb-id value", "id value")), document.crossRefs);

        assertEquals(Collections.singleton("pdb"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_pdb"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_pdb"));
        assertEquals(Set.of("PDB12345"), document.content);

        assertTrue(document.d3structure);
    }

    @Test
    void convertEmblCrossReferences() {
        GORepo goRelationRepo = mock(GORepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("EMBL"),
                        "id value",
                        new Property("ProteinId", "EMBL12345"),
                        new Property("NotProteinId", "notIndexed"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(
                        Arrays.asList("embl-id value", "embl-EMBL12345", "EMBL12345", "id value")),
                document.crossRefs);

        assertEquals(Collections.singleton("embl"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_embl"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_embl"));
        assertEquals(Set.of("notIndexed"), document.content);
    }

    @Test
    void convertCrossReferencesWithProperty() {
        GORepo goRelationRepo = mock(GORepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("Ensembl"),
                        "id value",
                        new Property("ProteinId", "E12345"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(
                        Arrays.asList("ensembl-id value", "ensembl-E12345", "E12345", "id value")),
                document.crossRefs);

        assertEquals(Collections.singleton("ensembl"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_ensembl"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_ensembl"));
        assertNotEquals(Set.of("E12345"), document.content);
    }

    @Test
    void convertTCDBCrossReferencesWithProperty() {
        GORepo goRelationRepo = mock(GORepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter =
                new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("TCDB"),
                        "8.A.94.1.2",
                        new Property("FamilyName", "the adiponectin (adiponectin) family"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(Arrays.asList("tcdb-8.A.94.1.2", "8", "8.A.94.1.2", "tcdb-8")),
                document.crossRefs);

        assertEquals(Collections.singleton("tcdb"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_tcdb"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_tcdb"));
        assertEquals(1, document.content.size());
        assertEquals("[the adiponectin (adiponectin) family]", document.content.toString());
    }

    private static UniProtKBCrossReference getUniProtDBCrossReference(
            UniProtKBDatabase dbType, String id, Property... property) {
        return new UniProtCrossReferenceBuilder()
                .id(id)
                .isoformId("Q9NXB0-1")
                .propertiesSet(Arrays.asList(property))
                .database(dbType)
                .evidencesAdd(createEvidence())
                .build();
    }

    private static Evidence createEvidence() {
        return new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000269)
                .databaseName("PubMed")
                .databaseId("71234329")
                .build();
    }
}
