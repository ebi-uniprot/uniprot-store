package org.uniprot.store.indexer.uniprotkb.converter;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.core.uniprot.xdb.UniProtDBCrossReference;
import org.uniprot.core.uniprot.xdb.UniProtXDbType;
import org.uniprot.core.uniprot.xdb.builder.UniProtDBCrossReferenceBuilder;
import org.uniprot.store.indexer.uniprot.go.GoRelationRepo;
import org.uniprot.store.indexer.uniprot.go.GoTermFileReader;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

/**
 * @author lgonzales
 * @since 2019-09-11
 */
class UniProtEntryCrossReferenceConverterTest {

    @Test
    void convertProteomeCrossReferences() {
        GoRelationRepo goRelationRepo = mock(GoRelationRepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtDBCrossReference xref = getUniProtDBCrossReference(new UniProtXDbType("Proteomes"), "id value", new Property("Component", "PC12345"));
        List<UniProtDBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        //Proteomes and proteomes components are not being saved in the content (default field), should it be?
        assertEquals(3, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("proteomes-id value", "proteomes", "id value")), document.content);

        assertEquals(new HashSet<>(Arrays.asList("proteomes-id value", "id value")), document.xrefs);

        assertEquals(Collections.singleton("proteomes"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_proteomes"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_proteomes"));

        assertEquals(Collections.singleton("id value"), document.proteomes);
        assertEquals(Collections.singleton("PC12345"), document.proteomeComponents);

    }

    @Test
    void convertGoCrossReferences() {
        GoRelationRepo goRelationRepo = mock(GoRelationRepo.class);
        when(goRelationRepo.getAncestors("GO:12345", asList(IS_A, PART_OF)))
                .thenReturn(Collections.singleton(new GoTermFileReader.GoTermImpl("GO:0097440", "GOTERM")));

        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtDBCrossReference xref = getUniProtDBCrossReference(new UniProtXDbType("GO"), "GO:12345",
                new Property("GoTerm", "C:apical dendrite"),
                new Property("GoEvidenceType", "IDA:UniProtKB"));
        List<UniProtDBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(5, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("go-GO:12345", "GO:12345", "go", "12345", "apical dendrite")), document.content);

        assertEquals(new HashSet<>(Arrays.asList("go-GO:12345", "GO:12345")), document.xrefs);

        assertEquals(Collections.singleton("go"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_go"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_go"));

        assertEquals(new HashSet<>(Arrays.asList("0097440", "12345", "GOTERM", "apical dendrite")), document.goes);
        assertEquals(new HashSet<>(Arrays.asList("0097440", "12345")), document.goIds);

        assertTrue(document.goWithEvidenceMaps.containsKey("go_ida"));
        assertEquals(new HashSet<>(Arrays.asList("0097440", "12345", "GOTERM", "apical dendrite")), document.goWithEvidenceMaps.get("go_ida"));
    }

    @Test
    void convertPDBCrossReferences() {
        GoRelationRepo goRelationRepo = mock(GoRelationRepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtDBCrossReference xref = getUniProtDBCrossReference(new UniProtXDbType("PDB"), "id value", new Property("id", "PDB12345"));
        List<UniProtDBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(3, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("pdb-id value", "pdb", "id value")), document.content);

        assertEquals(new HashSet<>(Arrays.asList("pdb-id value", "id value")), document.xrefs);

        assertEquals(Collections.singleton("pdb"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_pdb"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_pdb"));

        assertTrue(document.d3structure);
    }

    @Test
    void convertEmblCrossReferences() {
        GoRelationRepo goRelationRepo = mock(GoRelationRepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtDBCrossReference xref = getUniProtDBCrossReference(new UniProtXDbType("EMBL"), "id value", new Property("ProteinId", "EMBL12345"), new Property("NotProteinId", "notIndexed"));
        List<UniProtDBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(5, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("embl-id value", "embl", "embl-EMBL12345",
                "EMBL12345", "id value")), document.content);

        assertEquals(new HashSet<>(Arrays.asList("embl-id value", "embl-EMBL12345",
                "EMBL12345", "id value")), document.xrefs);

        assertEquals(Collections.singleton("embl"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_embl"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_embl"));
    }

    @Test
    void convertCrossReferencesWithProperty() {
        GoRelationRepo goRelationRepo = mock(GoRelationRepo.class);
        Map<String, SuggestDocument> suggestDocuments = new HashMap<>();

        UniProtDocument document = new UniProtDocument();

        UniProtEntryCrossReferenceConverter converter = new UniProtEntryCrossReferenceConverter(goRelationRepo, suggestDocuments);

        UniProtDBCrossReference xref = getUniProtDBCrossReference(new UniProtXDbType("Ensembl"), "id value", new Property("ProteinId", "E12345"));
        List<UniProtDBCrossReference> references = Collections.singletonList(xref);

        converter.convertCrossReferences(references, document);

        assertEquals(5, document.content.size());
        assertEquals(new HashSet<>(Arrays.asList("ensembl-id value", "ensembl", "ensembl-E12345",
                "E12345", "id value")), document.content);

        assertEquals(new HashSet<>(Arrays.asList("ensembl-id value", "ensembl-E12345",
                "E12345", "id value")), document.xrefs);

        assertEquals(Collections.singleton("ensembl"), document.databases);

        assertTrue(document.xrefCountMap.containsKey("xref_count_ensembl"));
        assertEquals(1L, document.xrefCountMap.get("xref_count_ensembl"));

    }

    private static UniProtDBCrossReference getUniProtDBCrossReference(UniProtXDbType dbType, String id, Property... property) {
        return new UniProtDBCrossReferenceBuilder()
                .id(id)
                .isoformId("Q9NXB0-1")
                .properties(Arrays.asList(property))
                .databaseType(dbType)
                .addEvidence(createEvidence())
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