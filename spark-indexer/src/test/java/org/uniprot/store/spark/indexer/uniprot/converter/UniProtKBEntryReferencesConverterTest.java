package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.CrossReference;
import org.uniprot.core.citation.Citation;
import org.uniprot.core.citation.CitationDatabase;
import org.uniprot.core.citation.impl.JournalArticleBuilder;
import org.uniprot.core.impl.CrossReferenceBuilder;
import org.uniprot.core.uniprotkb.ReferenceComment;
import org.uniprot.core.uniprotkb.ReferenceCommentType;
import org.uniprot.core.uniprotkb.UniProtKBReference;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.ReferenceCommentBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBReferenceBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-09
 */
class UniProtKBEntryReferencesConverterTest {

    @Test
    void convertCompleteReference() {
        List<UniProtKBReference> references = new ArrayList<>();
        references.add(getUniProtReference(ReferenceCommentType.PLASMID, "one"));
        references.add(getUniProtReference(ReferenceCommentType.PLASMID, "two"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // reference fields
        assertEquals(Arrays.asList("one  author", "two  author"), document.referenceAuthors);
        assertEquals(Arrays.asList("one  tittle", "two  tittle"), document.referenceTitles);
        assertEquals(Arrays.asList(get1Jan2029(), get1Jan2029()), document.referenceDates);
        assertEquals(
                Arrays.asList("one journal name", "two journal name"), document.referenceJournals);
        assertEquals(
                Arrays.asList("one auth group", "two auth group"), document.referenceOrganizations);
        assertEquals(Arrays.asList("oneid", "twoid"), document.referencePubmeds);
        assertEquals(
                new HashSet<>(Arrays.asList("one reference comment", "two reference comment")),
                document.rcPlasmid);
        assertEquals(new HashSet<>(Arrays.asList("one position", "two position")), document.scopes);
    }

    @Test
    void convertReferencesForTissue() {
        List<UniProtKBReference> references =
                Collections.singletonList(getUniProtReference(ReferenceCommentType.TISSUE, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcTissue);
    }

    @Test
    void convertReferencesForStrain() {
        List<UniProtKBReference> references =
                Collections.singletonList(getUniProtReference(ReferenceCommentType.STRAIN, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcStrain);
    }

    @Test
    void convertReferencesForTransposon() {
        List<UniProtKBReference> references =
                Collections.singletonList(
                        getUniProtReference(ReferenceCommentType.TRANSPOSON, "one"));
        UniProtDocument document = new UniProtDocument();

        UniProtEntryReferencesConverter converter = new UniProtEntryReferencesConverter();
        converter.convertReferences(references, document);

        // then
        assertEquals(
                new HashSet<>(Collections.singletonList("one reference comment")),
                document.rcTransposon);
    }

    private static UniProtKBReference getUniProtReference(
            ReferenceCommentType referenceCommentType, String prefix) {
        Evidence evidence =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000269)
                        .databaseName("PubMed")
                        .databaseId(prefix + "-dbid")
                        .build();

        CrossReference<CitationDatabase> xref =
                new CrossReferenceBuilder<CitationDatabase>()
                        .database(CitationDatabase.PUBMED)
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
                        .citationCrossReferencesSet(Collections.singletonList(xref))
                        .build();

        ReferenceComment referenceComment =
                new ReferenceCommentBuilder()
                        .type(referenceCommentType)
                        .value(prefix + " reference comment")
                        .evidencesSet(Collections.singletonList(evidence))
                        .build();

        return new UniProtKBReferenceBuilder()
                .citation(citation)
                .referenceCommentsAdd(referenceComment)
                .referencePositionsAdd(prefix + " position")
                .evidencesSet(Collections.singletonList(evidence))
                .build();
    }

    private Date get1Jan2029() {
        return Date.from(
                LocalDate.of(2029, Month.JANUARY, 1)
                        .atStartOfDay(ZoneId.systemDefault())
                        .toInstant());
    }
}
