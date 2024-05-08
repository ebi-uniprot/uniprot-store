package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.core.uniprotkb.xdb.UniProtKBDatabase;
import org.uniprot.core.uniprotkb.xdb.impl.UniProtCrossReferenceBuilder;
import org.uniprot.cv.xdb.UniProtKBDatabaseImpl;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtKBEntryCrossReferenceConverterTest {

    @Test
    void convertProteomeCrossReferences() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

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
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("GO"),
                        "GO:12345",
                        new Property("GoTerm", "C:apical dendrite"),
                        new Property("GoEvidenceType", "IDA:UniProtKB"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(Set.of("apical dendrite"), document.content);

        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "go-GO:12345", "GO:12345", "go-GO", "GO", "12345", "go-12345")),
                document.crossRefs);

        assertEquals(Collections.singleton("go"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_go"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_go"));

        assertEquals(new HashSet<>(Arrays.asList("12345", "apical dendrite")), document.goes);
        assertEquals(new HashSet<>(Collections.singletonList("12345")), document.goIds);

        assertTrue(document.goWithEvidenceMaps.containsKey("go_ida"));
        assertEquals(
                new HashSet<>(Arrays.asList("12345", "apical dendrite")),
                document.goWithEvidenceMaps.get("go_ida"));
        assertEquals(Set.of("apical dendrite"), document.content);
    }

    @Test
    void convertPDBCrossReferences() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

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

        assertTrue(document.d3structure);
        assertEquals(Set.of("PDB12345"), document.content);
    }

    @Test
    void convertEmblCrossReferences() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("EMBL"),
                        "id value",
                        new Property("ProteinId", "EMBL12345"),
                        new Property("Status", "JOINED"),
                        new Property("MoleculeType", "Genomic_DNA"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "embl-Genomic_DNA",
                                "embl-id value",
                                "EMBL12345",
                                "Genomic_DNA",
                                "embl-EMBL12345",
                                "JOINED",
                                "id value",
                                "embl-JOINED")),
                document.crossRefs);

        assertEquals(Collections.singleton("embl"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_embl"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_embl"));
        assertTrue(document.content.isEmpty());
    }

    @Test
    void convertCrossReferencesWithProperty() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

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
        assertFalse(document.content.contains("E12345"));
    }

    @Test
    void convertTCDBCrossReferencesWithProperty() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

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

    @Test
    void convertCDDCrossReferencesWithProperty() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("CDD"),
                        "cd12087",
                        new Property("EntryName", "TM_EGFR-like"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "TM_EGFR-like", "cd12087", "cdd-cd12087", "cdd-TM_EGFR-like")),
                document.crossRefs);

        assertEquals(Collections.singleton("cdd"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_cdd"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_cdd"));
        assertEquals(0, document.content.size());
    }

    @Test
    void convertHGNCCrossReferencesWithProperty() {
        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter();

        UniProtKBCrossReference xref =
                getUniProtDBCrossReference(
                        new UniProtKBDatabaseImpl("HGNC"),
                        "HGNC:5984",
                        new Property("GeneName", "IL17D"));
        List<UniProtKBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "HGNC:5984",
                                "hgnc-HGNC",
                                "hgnc-HGNC:5984",
                                "HGNC",
                                "5984",
                                "hgnc-5984")),
                document.crossRefs);

        assertEquals(Collections.singleton("hgnc"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_hgnc"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_hgnc"));
        assertEquals(1, document.content.size());
        assertEquals("[IL17D]", document.content.toString());
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
