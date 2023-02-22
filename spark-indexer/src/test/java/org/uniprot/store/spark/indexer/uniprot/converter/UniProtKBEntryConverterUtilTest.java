package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.Author;
import org.uniprot.core.citation.impl.AuthorBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.comment.APIsoform;
import org.uniprot.core.uniprotkb.comment.AlternativeProductsComment;
import org.uniprot.core.uniprotkb.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprotkb.comment.impl.APIsoformBuilder;
import org.uniprot.core.uniprotkb.comment.impl.AlternativeProductsCommentBuilder;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDictionary;

/**
 * @author lgonzales
 * @since 2019-09-09
 */
class UniProtKBEntryConverterUtilTest {

    @Test
    void emptyEvidenceDoNotAddExceptionalEvidence() {
        List<Evidence> evidences = new ArrayList<>();

        Set<String> extractedEvidences = UniProtEntryConverterUtil.extractEvidence(evidences);
        assertNotNull(extractedEvidences);
        assertTrue(extractedEvidences.isEmpty());
    }

    @Test
    void extractAutomaticEvidence() {
        List<Evidence> evidences = new ArrayList<>();
        evidences.add(
                new EvidenceBuilder()
                        .databaseId("id")
                        .databaseName("name")
                        .evidenceCode(EvidenceCode.ECO_0000213)
                        .build());

        Set<String> extractedEvidences = UniProtEntryConverterUtil.extractEvidence(evidences);

        assertEquals(2, extractedEvidences.size());
        assertTrue(extractedEvidences.contains(EvidenceCode.ECO_0000213.name()));
        assertTrue(extractedEvidences.contains("automatic"));
    }

    @Test
    void extractManualEvidence() {
        List<Evidence> evidences = new ArrayList<>();
        evidences.add(
                new EvidenceBuilder()
                        .databaseId("id")
                        .databaseName("name")
                        .evidenceCode(EvidenceCode.ECO_0000244)
                        .build());

        Set<String> extractedEvidences = UniProtEntryConverterUtil.extractEvidence(evidences);

        assertEquals(2, extractedEvidences.size());
        assertTrue(extractedEvidences.contains(EvidenceCode.ECO_0000244.name()));
        assertTrue(extractedEvidences.contains("manual"));
    }

    @Test
    void extractExperimentalEvidence() {
        List<Evidence> evidences = new ArrayList<>();
        evidences.add(
                new EvidenceBuilder()
                        .databaseId("id")
                        .databaseName("name")
                        .evidenceCode(EvidenceCode.ECO_0000303)
                        .build());

        Set<String> extractedEvidences = UniProtEntryConverterUtil.extractEvidence(evidences);

        assertEquals(3, extractedEvidences.size());
        assertTrue(extractedEvidences.contains(EvidenceCode.ECO_0000303.name()));
        assertTrue(extractedEvidences.contains("experimental"));
        assertTrue(extractedEvidences.contains("manual"));
    }

    @Test
    void createSuggestionMapKey() {
        String mapKey =
                UniProtEntryConverterUtil.createSuggestionMapKey(SuggestDictionary.GO, "12345");
        assertEquals("GO:12345", mapKey);
    }

    @Test
    void getXrefId() {
        List<String> result = UniProtEntryConverterUtil.getXrefId("12345", "DB_NAME");

        assertEquals(2, result.size());
        assertEquals("12345", result.get(0));
        assertEquals("DB_NAME-12345", result.get(1));
    }

    @Test
    void getXrefIdWithDot() {
        List<String> result = UniProtEntryConverterUtil.getXrefId("12345.1", "DB_NAME");

        assertEquals(4, result.size());
        assertEquals("12345.1", result.get(0));
        assertEquals("DB_NAME-12345.1", result.get(1));
        assertEquals("12345", result.get(2));
        assertEquals("DB_NAME-12345", result.get(3));
    }

    @Test
    void truncatedSortValue() {
        String value = "1234567890123456789012345678901234567890";
        assertEquals(
                "123456789012345678901234567890",
                UniProtEntryConverterUtil.truncatedSortValue(value));
    }

    @Test
    void truncatedSortValueNull() {
        assertNull(UniProtEntryConverterUtil.truncatedSortValue(null));
    }

    @Test
    void addValueListToStringList() {
        List<Author> authors = new ArrayList<>();
        authors.add(new AuthorBuilder("Author name 1").build());
        authors.add(new AuthorBuilder("Author name 2").build());
        List<String> result = new ArrayList<>();

        UniProtEntryConverterUtil.addValueListToStringList(result, authors);

        assertEquals(2, result.size());
        assertEquals("Author name 1", result.get(0));
        assertEquals("Author name 2", result.get(1));
    }

    @Test
    void addValueToStringList() {
        List<String> result = new ArrayList<>();

        UniProtEntryConverterUtil.addValueToStringList(
                result, new AuthorBuilder("Author name").build());

        assertEquals(1, result.size());
        assertEquals("Author name", result.get(0));
    }

    @Test
    void isCanonicalIsoformNotCannonical() {
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
                        .build();

        boolean isCanonical = UniProtEntryConverterUtil.isCanonicalIsoform(entry);
        assertFalse(isCanonical);
    }

    @Test
    void isCanonicalIsoformWhenIsCannonicalIsoform() {
        APIsoform isoform =
                new APIsoformBuilder()
                        .isoformIdsAdd("P12345")
                        .sequenceStatus(IsoformSequenceStatus.DISPLAYED)
                        .build();

        AlternativeProductsComment comment =
                new AlternativeProductsCommentBuilder().isoformsAdd(isoform).build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "ID_P12345", UniProtKBEntryType.SWISSPROT)
                        .commentsAdd(comment)
                        .build();

        boolean isCanonical = UniProtEntryConverterUtil.isCanonicalIsoform(entry);
        assertTrue(isCanonical);
    }

    @Test
    void canAddExperimentalByAnnotationText() {
        assertTrue(UniProtEntryConverterUtil.canAddExperimentalByAnnotationText("test value."));
    }

    @Test
    void canAddExperimentalByAnnotationTextBySimilarity() {
        assertFalse(
                UniProtEntryConverterUtil.canAddExperimentalByAnnotationText(
                        "test (By Similarity)."));
    }

    @Test
    void canAddExperimentalByAnnotationTextProbable() {
        assertFalse(
                UniProtEntryConverterUtil.canAddExperimentalByAnnotationText("test (Probable)."));
    }

    @Test
    void canAddExperimentalByAnnotationTextPotential() {
        assertFalse(
                UniProtEntryConverterUtil.canAddExperimentalByAnnotationText("test (Potential)."));
    }

    @Test
    void hasExperimentalEvidenceWithoutExperimental() {
        assertFalse(UniProtEntryConverterUtil.hasExperimentalEvidence(List.of("ECO_0000305")));
    }

    @Test
    void hasExperimentalEvidenceWithExperimental() {
        assertTrue(UniProtEntryConverterUtil.hasExperimentalEvidence(List.of("ECO_0000269")));
    }

    @Test
    void canAddExperimentalWithExperimental() {
        assertTrue(UniProtEntryConverterUtil.canAddExperimental(false, "test (Potential).", false, Set.of("ECO_0000269")));
    }

    @Test
    void canAddExperimentalWithoutExperimentalButValidImplicitEvidence() {
        assertTrue(UniProtEntryConverterUtil.canAddExperimental(true, "Valid Text", true, Collections.emptySet()));
    }

    @Test
    void canNotAddExperimentalWithoutExperimentalAndNotExperimentalType() {
        assertFalse(UniProtEntryConverterUtil.canAddExperimental(false, "Valid Text", true, Collections.emptySet()));
    }

    @Test
    void canNotAddExperimentalWithoutExperimentalAndNotValidText() {
        assertFalse(UniProtEntryConverterUtil.canAddExperimental(true, "test (Potential).", true, Collections.emptySet()));
    }

    @Test
    void canNotAddExperimentalWithoutExperimentalAndNotReviewed() {
        assertFalse(UniProtEntryConverterUtil.canAddExperimental(true, "Valid Text", false, Collections.emptySet()));
    }

}
