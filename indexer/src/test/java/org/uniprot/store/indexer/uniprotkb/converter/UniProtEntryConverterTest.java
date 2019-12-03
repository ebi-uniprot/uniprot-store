package org.uniprot.store.indexer.uniprotkb.converter;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniprot.*;
import org.uniprot.core.uniprot.builder.*;
import org.uniprot.core.uniprot.comment.APIsoform;
import org.uniprot.core.uniprot.comment.AlternativeProductsComment;
import org.uniprot.core.uniprot.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprot.comment.builder.APCommentBuilder;
import org.uniprot.core.uniprot.comment.builder.APIsoformBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.builder.EvidenceBuilder;
import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2019-09-16
 */
class UniProtEntryConverterTest {

    @Test
    void documentConversionException() {
        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);

        assertThrows(DocumentConversionException.class, () -> {
            converter.convert(null);
        });
    }

    @Test
    void convertCanonicalAccessionEntry() {
        // given
        UniProtEntry entry = new UniProtEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtEntryType.TREMBL)
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertFalse(document.isIsoform);
        assertTrue(document.active);
        assertTrue(document.secacc.isEmpty());
        assertEquals(new HashSet<>(Arrays.asList("P12345", "UNIPROT_ENTRYID")), document.content);
    }

    @Test
    void convertIsoformAccessionEntry() {
        // given
        UniProtEntry entry = new UniProtEntryBuilder("P12345-5", "UNIPROT_ENTRYID", UniProtEntryType.TREMBL)
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-5", document.accession);
        assertTrue(document.isIsoform);
        assertTrue(document.active);
        assertEquals(Collections.singletonList("P12345"), document.secacc);
        assertEquals(new HashSet<>(Arrays.asList("P12345-5", "P12345", "UNIPROT_ENTRYID")), document.content);
    }

    @Test
    void convertCanonicalIsoformAccessionEntry() {
        // given
        APIsoform isoform = new APIsoformBuilder()
                .addId("P12345-1")
                .sequenceStatus(IsoformSequenceStatus.DISPLAYED)
                .build();

        AlternativeProductsComment comment = new APCommentBuilder()
                .addIsoform(isoform)
                .build();

        UniProtEntry entry = new UniProtEntryBuilder("P12345-1", "UNIPROT_ENTRYID", UniProtEntryType.SWISSPROT)
                .commentsSet(Collections.singletonList(comment))
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-1", document.accession);
        assertNull(document.isIsoform);
        assertNull(document.reviewed);
        assertEquals(Collections.emptySet(), document.content);
    }

    @Test
    void convertIdDefaultForTrEMBLIncludesSpeciesButNotAccession() {
        // given
        String species = "SPECIES";
        UniProtEntry entry = new UniProtEntryBuilder("P12345", "ACCESSION_" + species, UniProtEntryType.TREMBL)
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals(species, document.idDefault);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "ACCESSION_SPECIES")), document.content);
    }

    @Test
    void convertIdDefaultForSwissProtIncludesGeneAndSpecies() {
        // given
        String id = "GENE_SPECIES";
        UniProtEntry entry = new UniProtEntryBuilder("P12345", id, UniProtEntryType.SWISSPROT)
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals(id, document.idDefault);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "GENE_SPECIES")), document.content);
    }

    @Test
    void convertEntryAuditFields() {
        // given
        LocalDate firstPublic = LocalDate.of(2000, 1, 25);
        LocalDate lastAnnotationUpdate = LocalDate.of(2019, 1, 25);
        LocalDate lastSequenceUpdate = LocalDate.of(2018, 1, 25);

        EntryAudit entryAudit = new EntryAuditBuilder()
                .entryVersion(10)
                .firstPublic(firstPublic)
                .lastAnnotationUpdate(lastAnnotationUpdate)
                .sequenceVersion(5)
                .lastSequenceUpdate(lastSequenceUpdate)
                .build();

        UniProtEntry entry = getBasicEntryBuilder()
                .entryAudit(entryAudit)
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(firstPublic, getLocalDateFromDate(document.firstCreated));
        assertEquals(lastAnnotationUpdate, getLocalDateFromDate(document.lastModified));
        assertEquals(lastSequenceUpdate, getLocalDateFromDate(document.sequenceUpdated));
    }

    @Test
    void convertGeneNamesFields() {
        // given
        Gene gene = new GeneBuilder()
                .geneName(new GeneNameBuilder().value("some Gene name").build())
                .addSynonyms(new GeneNameSynonymBuilder().value("some Syn").build())
                .addOrderedLocusNames(new OrderedLocusNameBuilder().value("some locus").build())
                .addOrfNames(new ORFNameBuilder().value("some orf").build())
                .addOrfNames(new ORFNameBuilder().value("some other orf").build())
                .build();

        UniProtEntry entry = getBasicEntryBuilder()
                .genesSet(Collections.singletonList(gene))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("some Gene name", "some Syn", "some locus", "some orf", "some other orf"), document.geneNamesExact);
        assertEquals(document.geneNamesExact, document.geneNames);
        assertEquals("some Gene name some Syn some l", document.geneNamesSort);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "UNIPROT_ENTRYID", "some Gene name",
                "some Syn", "some locus", "some orf", "some other orf")), document.content);
    }

    @Test
    void convertKeywordsFields() {
        // given
        Keyword keyword = new KeywordBuilder()
                .id("KW-1111")
                .value("keyword value")
                .category(KeywordCategory.DOMAIN)
                .addEvidence(createEvidence("50"))
                .build();

        UniProtEntry entry = getBasicEntryBuilder()
                .keywordAdd(keyword)
                .build();

        Map<String, SuggestDocument> suggestions = new HashMap<>();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, suggestions);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("KW-1111", "keyword value", "KW-9994", "Domain"), document.keywords);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "UNIPROT_ENTRYID",
                "KW-1111", "keyword value", "KW-9994", "Domain")), document.content);

        //check suggestions
        assertEquals(2, suggestions.size());
        assertTrue(suggestions.containsKey("KEYWORD:KW-1111"));
        assertTrue(suggestions.containsKey("KEYWORD:KW-9994"));

        SuggestDocument suggestDocument = suggestions.get("KEYWORD:KW-1111");
        assertEquals("KEYWORD", suggestDocument.dictionary);
        assertEquals("KW-1111", suggestDocument.id);
        assertEquals("keyword value", suggestDocument.value);
        assertNotNull(suggestDocument.altValues);
        assertEquals(0, suggestDocument.altValues.size());
    }

    @Test
    void convertOrganelleFields() {
        // given
        GeneLocation geneLocation = new GeneLocationBuilder()
                .geneEncodingType(GeneEncodingType.CYANELLE)
                .value("geneLocation value")
                .addEvidence(createEvidence("60"))
                .build();

        UniProtEntry entry = getBasicEntryBuilder()
                .geneLocationAdd(geneLocation)
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("plastid", "cyanelle"), document.organelles);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "UNIPROT_ENTRYID",
                "cyanelle", "plastid")), document.content);
    }

    @Test
    void convertProteinExistenceFields() {
        // given
        UniProtEntry entry = getBasicEntryBuilder()
                .proteinExistence(ProteinExistence.PROTEIN_LEVEL)
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals("PROTEIN_LEVEL", document.proteinExistence);
        //@lgonzales: protein existence information is not in the content (default) field, should it be?
    }

    @Test
    void convertSequenceFields() {
        // given
        UniProtEntry entry = getBasicEntryBuilder()
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(5, document.seqLength);
        assertEquals(373, document.seqMass);
    }

    @Test
    void convertEntryScore() {
        // given
        UniProtEntry entry = getBasicEntryBuilder()
                .sequence(new SequenceImpl("AAAAA"))
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(1, document.score);
    }

    @Test
    void convertEvidenceSources() {
        // given
        Evidence evidence = new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000256)
                .databaseName("HAMAP-Rule")
                .databaseId("hamapId")
                .build();

        Gene gene = new GeneBuilder()
                .geneName(new GeneNameBuilder().value("some Gene name").addEvidence(evidence).build())
                .build();
        UniProtEntry entry = getBasicEntryBuilder()
                .geneAdd(gene)
                .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Collections.singletonList("hamap"), document.sources);
    }

    private UniProtEntryBuilder getBasicEntryBuilder() {
        return new UniProtEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtEntryType.SWISSPROT)
                .sequence(new SequenceImpl("AAAAA"));
    }

    private LocalDate getLocalDateFromDate(Date date) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return localDateTime.toLocalDate();
    }

    private Evidence createEvidence(String posfix) {
        return new EvidenceBuilder()
                .evidenceCode(EvidenceCode.ECO_0000256)
                .databaseName("PubMed")
                .databaseId("id" + posfix)
                .build();
    }
}