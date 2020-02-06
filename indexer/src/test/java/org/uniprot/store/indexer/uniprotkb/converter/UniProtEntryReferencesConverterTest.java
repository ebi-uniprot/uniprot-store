package org.uniprot.store.indexer.uniprotkb.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.DBCrossReference;
import org.uniprot.core.builder.DBCrossReferenceBuilder;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationXrefType;
import org.uniprot.core.citation.builder.JournalArticleBuilder;
import org.uniprot.core.uniprot.ReferenceComment;
import org.uniprot.core.uniprot.ReferenceCommentType;
import org.uniprot.core.uniprot.UniProtReference;
import org.uniprot.core.uniprot.builder.ReferenceCommentBuilder;
import org.uniprot.core.uniprot.builder.UniProtReferenceBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-09
 */
class UniProtEntryReferencesConverterTest {

    @Test
    void convertCompleteReference() {
        List<UniProtReference> references = new ArrayList<>();
        references.add(getUniProtReference(ReferenceCommentType.PLASMID, "one"));
        references.add(getUniProtReference(ReferenceCommentType.PLASMID, "two"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // reference fields
        assertEquals(Arrays.asList("one  author", "two  author"), document.referenceAuthors);
        assertEquals(Arrays.asList("one  tittle", "two  tittle"), document.referenceTitles);
        assertEquals(
                Arrays.asList(new Date(1861920000000L), new Date(1861920000000L)),
                document.referenceDates);
        assertEquals(
                Arrays.asList("one journal name", "two journal name"), document.referenceJournals);
        assertEquals(
                Arrays.asList("one auth group", "two auth group"), document.referenceOrganizations);
        assertEquals(Arrays.asList("oneid", "twoid"), document.referencePubmeds);
        assertEquals(
                new HashSet<>(Arrays.asList("one reference comment", "two reference comment")),
                document.rcPlasmid);
        assertEquals(new HashSet<>(Arrays.asList("one position", "two position")), document.scopes);

        // content field
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "one  author",
                                "two  author",
                                "one  tittle",
                                "two  tittle",
                                "one journal name",
                                "two journal name",
                                "one auth group",
                                "two auth group",
                                "oneid",
                                "twoid",
                                "one reference comment",
                                "two reference comment",
                                "one position",
                                "two position")),
                document.content);
    }

    @Test
    void convertReferencesForTissue() {
        List<UniProtReference> references =
                Collections.singletonList(getUniProtReference(ReferenceCommentType.TISSUE, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcTissue);
        assertTrue(document.content.contains("one reference comment"));
    }

    @Test
    void convertReferencesForStrain() {
        List<UniProtReference> references =
                Collections.singletonList(getUniProtReference(ReferenceCommentType.STRAIN, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcStrain);
        assertTrue(document.content.contains("one reference comment"));
    }

    @Test
    void convertReferencesForTransposon() {
        List<UniProtReference> references =
                Collections.singletonList(
                        getUniProtReference(ReferenceCommentType.TRANSPOSON, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcTransposon);
        assertTrue(document.content.contains("one reference comment"));
    }

    private static UniProtReference getUniProtReference(
            ReferenceCommentType referenceCommentType, String prefix) {
        Evidence evidence =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000269)
                        .databaseName("PubMed")
                        .databaseId(prefix + "-dbid")
                        .build();

        DBCrossReference<CitationXrefType> xref =
                new DBCrossReferenceBuilder<CitationXrefType>()
                        .databaseType(CitationXrefType.PUBMED)
                        .id(prefix + "id")
                        .build();

        Citation citation =
                new JournalArticleBuilder()
                        .journalName(prefix + " journal name")
                        .firstPage("1")
                        .lastPage("10")
                        .volume("volume value")
                        .publicationDate("2029")
                        .authoringGroupsAdd(prefix + " auth group")
                        .authorsAdd(prefix + "  author")
                        .title(prefix + "  tittle")
                        .citationXrefsSet(Collections.singletonList(xref))
                        .build();

        ReferenceComment referenceComment =
                new ReferenceCommentBuilder()
                        .type(referenceCommentType)
                        .value(prefix + " reference comment")
                        .evidencesSet(Collections.singletonList(evidence))
                        .build();

        return new UniProtReferenceBuilder()
                .citation(citation)
                .referenceCommentsAdd(referenceComment)
                .referencePositionsAdd(prefix + " position")
                .evidencesSet(Collections.singletonList(evidence))
                .build();
    }
}
