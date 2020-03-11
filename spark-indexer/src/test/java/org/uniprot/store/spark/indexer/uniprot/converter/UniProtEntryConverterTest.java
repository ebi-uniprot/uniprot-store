package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniprot.*;
import org.uniprot.core.uniprot.comment.APIsoform;
import org.uniprot.core.uniprot.comment.AlternativeProductsComment;
import org.uniprot.core.uniprot.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprot.comment.impl.AlternativeProductsCommentBuilder;
import org.uniprot.core.uniprot.comment.impl.APIsoformBuilder;
import org.uniprot.core.uniprot.evidence.Evidence;
import org.uniprot.core.uniprot.evidence.EvidenceCode;
import org.uniprot.core.uniprot.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprot.impl.*;
import org.uniprot.store.job.common.DocumentConversionException;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-16
 */
class UniProtEntryConverterTest {

    @Test
    void documentConversionException() {
        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);

        assertThrows(
                DocumentConversionException.class,
                () -> {
                    converter.convert(null);
                });
    }

    @Test
    void convertCanonicalAccessionEntry() {
        // given
        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
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
        UniProtEntry entry =
                new UniProtEntryBuilder("P12345-5", "UNIPROT_ENTRYID", UniProtEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-5", document.accession);
        assertTrue(document.isIsoform);
        assertTrue(document.active);
        assertEquals(Collections.singletonList("P12345"), document.secacc);
        assertEquals(
                new HashSet<>(Arrays.asList("P12345-5", "P12345", "UNIPROT_ENTRYID")),
                document.content);
    }

    @Test
    void convertCanonicalIsoformAccessionEntry() {
        // given
        APIsoform isoform =
                new APIsoformBuilder()
                        .isoformIdsAdd("P12345-1")
                        .sequenceStatus(IsoformSequenceStatus.DISPLAYED)
                        .build();

        AlternativeProductsComment comment = new AlternativeProductsCommentBuilder().isoformsAdd(isoform).build();

        UniProtEntry entry =
                new UniProtEntryBuilder("P12345-1", "UNIPROT_ENTRYID", UniProtEntryType.SWISSPROT)
                        .commentsSet(Collections.singletonList(comment))
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
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
        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", "ACCESSION_" + species, UniProtEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals(species, document.idDefault);
        assertEquals(new HashSet<>(Arrays.asList("P12345", "ACCESSION_SPECIES")), document.content);
    }

    @Test
    void convertIdDefaultForSwissProtIncludesGeneAndSpecies() {
        // given
        String id = "GENE_SPECIES";
        UniProtEntry entry =
                new UniProtEntryBuilder("P12345", id, UniProtEntryType.SWISSPROT)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
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

        EntryAudit entryAudit =
                new EntryAuditBuilder()
                        .entryVersion(10)
                        .firstPublic(firstPublic)
                        .lastAnnotationUpdate(lastAnnotationUpdate)
                        .sequenceVersion(5)
                        .lastSequenceUpdate(lastSequenceUpdate)
                        .build();

        UniProtEntry entry = getBasicEntryBuilder().entryAudit(entryAudit).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
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
        Gene gene =
                new GeneBuilder()
                        .geneName(new GeneNameBuilder().value("some Gene name").build())
                        .synonymsAdd(new GeneNameSynonymBuilder().value("some Syn").build())
                        .orderedLocusNamesAdd(
                                new OrderedLocusNameBuilder().value("some locus").build())
                        .orfNamesAdd(new ORFNameBuilder().value("some orf").build())
                        .orfNamesAdd(new ORFNameBuilder().value("some other orf").build())
                        .build();

        UniProtEntry entry =
                getBasicEntryBuilder().genesSet(Collections.singletonList(gene)).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(
                Arrays.asList(
                        "some Gene name", "some Syn", "some locus", "some orf", "some other orf"),
                document.geneNamesExact);
        assertEquals(document.geneNamesExact, document.geneNames);
        assertEquals("some Gene name some Syn some l", document.geneNamesSort);
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "P12345",
                                "UNIPROT_ENTRYID",
                                "some Gene name",
                                "some Syn",
                                "some locus",
                                "some orf",
                                "some other orf")),
                document.content);
    }

    @Test
    void convertKeywordsFields() {
        // given
        Keyword keyword =
                new KeywordBuilder()
                        .id("KW-1111")
                        .name("keyword value")
                        .category(KeywordCategory.DOMAIN)
                        .evidencesAdd(createEvidence("50"))
                        .build();

        UniProtEntry entry = getBasicEntryBuilder().keywordsAdd(keyword).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(
                Arrays.asList("KW-1111", "keyword value", "KW-9994", "Domain"), document.keywords);
        assertEquals(
                new HashSet<>(
                        Arrays.asList(
                                "P12345",
                                "UNIPROT_ENTRYID",
                                "KW-1111",
                                "keyword value",
                                "KW-9994",
                                "Domain")),
                document.content);
    }

    @Test
    void convertOrganelleFields() {
        // given
        GeneLocation geneLocation =
                new GeneLocationBuilder()
                        .geneEncodingType(GeneEncodingType.CYANELLE)
                        .value("geneLocation value")
                        .evidencesAdd(createEvidence("60"))
                        .build();

        UniProtEntry entry = getBasicEntryBuilder().geneLocationsAdd(geneLocation).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("plastid", "cyanelle"), document.organelles);
        assertEquals(
                new HashSet<>(Arrays.asList("P12345", "UNIPROT_ENTRYID", "cyanelle", "plastid")),
                document.content);
    }

    @Test
    void convertProteinExistenceFields() {
        // given
        UniProtEntry entry =
                getBasicEntryBuilder().proteinExistence(ProteinExistence.PROTEIN_LEVEL).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals("PROTEIN_LEVEL", document.proteinExistence);
        // @lgonzales: protein existence information is not in the content (default) field, should
        // it be?
    }

    @Test
    void convertSequenceFields() {
        // given
        UniProtEntry entry = getBasicEntryBuilder().build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(5, document.seqLength);
        assertEquals(373, document.seqMass);
    }

    @Test
    void convertEntryScore() {
        // given
        UniProtEntry entry =
                getBasicEntryBuilder().sequence(new SequenceBuilder("AAAAA").build()).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(1, document.score);
    }

    @Test
    void convertEvidenceSources() {
        // given
        Evidence evidence =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000256)
                        .databaseName("HAMAP-Rule")
                        .databaseId("hamapId")
                        .build();

        Gene gene =
                new GeneBuilder()
                        .geneName(
                                new GeneNameBuilder()
                                        .value("some Gene name")
                                        .evidencesAdd(evidence)
                                        .build())
                        .build();
        UniProtEntry entry = getBasicEntryBuilder().genesAdd(gene).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Collections.singletonList("hamap"), document.sources);
    }

    private UniProtEntryBuilder getBasicEntryBuilder() {
        return new UniProtEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtEntryType.SWISSPROT)
                .sequence(new SequenceBuilder("AAAAA").build());
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
