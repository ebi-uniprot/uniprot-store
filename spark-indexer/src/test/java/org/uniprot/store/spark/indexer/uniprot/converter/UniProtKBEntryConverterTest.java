package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.*;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordCategory;
import org.uniprot.core.gene.Gene;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniprotkb.*;
import org.uniprot.core.uniprotkb.comment.APIsoform;
import org.uniprot.core.uniprotkb.comment.AlternativeProductsComment;
import org.uniprot.core.uniprotkb.comment.IsoformSequenceStatus;
import org.uniprot.core.uniprotkb.comment.impl.APIsoformBuilder;
import org.uniprot.core.uniprotkb.comment.impl.AlternativeProductsCommentBuilder;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.impl.EvidenceBuilder;
import org.uniprot.core.uniprotkb.impl.*;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-16
 */
class UniProtKBEntryConverterTest {

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
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtKBEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals("P12345-1", document.canonicalAccession);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
        assertEquals("UNIPROT_ENTRYID", document.idSort);
        assertFalse(document.isIsoform);
        assertTrue(document.active);
        assertTrue(document.secacc.isEmpty());
    }

    @Test
    void convertCanonicalAccessionWithIsoformsEntry() {
        // given
        AlternativeProductsComment alternativeproducts =
                new AlternativeProductsCommentBuilder().build();
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P21802", "UNIPROT_ENTRYID", UniProtKBEntryType.TREMBL)
                        .secondaryAccessionsAdd(new UniProtKBAccessionBuilder("P21803").build())
                        .commentsAdd(alternativeproducts)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P21802", document.accession);
        assertNull(document.canonicalAccession);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
        assertEquals("UNIPROT_ENTRYID", document.idSort);
        assertFalse(document.isIsoform);
        assertTrue(document.active);
        assertTrue(document.secacc.contains("P21803"));
    }

    @Test
    void convertIsoformAccessionEntry() {
        // given
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345-5", "UNIPROT_ENTRYID", UniProtKBEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-5", document.accession);
        assertTrue(document.isIsoform);
        assertTrue(document.active);
        assertEquals("P12345", document.canonicalAccession);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
        assertEquals("UNIPROT_ENTRYID", document.idSort);
    }

    @Test
    void convertCanonicalIsoformAccessionEntry() {
        // given
        APIsoform isoform =
                new APIsoformBuilder()
                        .isoformIdsAdd("P12345-1")
                        .sequenceStatus(IsoformSequenceStatus.DISPLAYED)
                        .build();

        AlternativeProductsComment comment =
                new AlternativeProductsCommentBuilder().isoformsAdd(isoform).build();

        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(
                                "P12345-1", "UNIPROT_ENTRYID", UniProtKBEntryType.SWISSPROT)
                        .commentsSet(Collections.singletonList(comment))
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-1", document.accession);
        assertNull(document.canonicalAccession);
        assertNull(document.isIsoform);
        assertNull(document.reviewed);
        assertEquals(Collections.emptySet(), document.content);
    }

    @Test
    void convertIdDefaultForTrEMBLIncludesSpeciesButNotAccession() {
        // given
        String species = "SPECIES";
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(
                                "P12345", "ACCESSION_" + species, UniProtKBEntryType.TREMBL)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals(Arrays.asList(species), document.idDefault);
    }

    @Test
    void convertIdDefaultForSwissProtIncludesGeneAndSpecies() {
        // given
        String id = "GENE_SPECIES";
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345", id, UniProtKBEntryType.SWISSPROT)
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals(Arrays.asList(id), document.idDefault);
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

        UniProtKBEntry entry = getBasicEntryBuilder().entryAudit(entryAudit).build();

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

        UniProtKBEntry entry =
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

        UniProtKBEntry entry = getBasicEntryBuilder().keywordsAdd(keyword).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(
                Arrays.asList("KW-1111", "keyword value", "KW-9994", "Domain"), document.keywords);
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

        UniProtKBEntry entry = getBasicEntryBuilder().geneLocationsAdd(geneLocation).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("plastid", "cyanelle"), document.organelles);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
        assertEquals("UNIPROT_ENTRYID", document.idSort);
    }

    @Test
    void convertProteinExistenceFields() {
        // given
        UniProtKBEntry entry =
                getBasicEntryBuilder().proteinExistence(ProteinExistence.PROTEIN_LEVEL).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(1, document.proteinExistence);
        // @lgonzales: protein existence information is not in the content (default) field, should
        // it be?
    }

    @Test
    void convertSequenceFields() {
        // given
        UniProtKBEntry entry = getBasicEntryBuilder().build();

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
        UniProtKBEntry entry =
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

        Evidence evidence2 =
                new EvidenceBuilder()
                        .evidenceCode(EvidenceCode.ECO_0000245)
                        .databaseName("PDB")
                        .databaseId("PDBId")
                        .build();

        Gene gene =
                new GeneBuilder()
                        .geneName(
                                new GeneNameBuilder()
                                        .value("some Gene name")
                                        .evidencesAdd(evidence)
                                        .evidencesAdd(evidence2)
                                        .build())
                        .build();
        UniProtKBEntry entry = getBasicEntryBuilder().genesAdd(gene).build();

        // when
        UniProtEntryConverter converter = new UniProtEntryConverter(null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(4, document.sources.size());
        assertTrue(document.sources.contains("hamap"));
        assertTrue(document.sources.contains("hamapid"));
        assertTrue(document.sources.contains("pdbid"));
        assertTrue(document.sources.contains("pdb"));
    }

    private UniProtKBEntryBuilder getBasicEntryBuilder() {
        return new UniProtKBEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtKBEntryType.SWISSPROT)
                .sequence(new SequenceBuilder("AAAAA").build());
    }

    private LocalDate getLocalDateFromDate(Date date) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
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
