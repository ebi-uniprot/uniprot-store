package org.uniprot.store.indexer.uniprotkb.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.*;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
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
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-16
 */
class UniProtKBEntryConverterTest {

    @Test
    void documentConversionException() {
        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);

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
                        .sequence(sq("AAAAA"))
                        .build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
        assertFalse(document.isIsoform);
        assertTrue(document.active);
        assertTrue(document.secacc.isEmpty());
    }

    @Test
    void convertIsoformAccessionEntry() {
        // given
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder("P12345-5", "UNIPROT_ENTRYID", UniProtKBEntryType.TREMBL)
                        .sequence(sq("AAAAA"))
                        .build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345-5", document.accession);
        assertTrue(document.isIsoform);
        assertTrue(document.active);
        assertEquals("P12345", document.canonicalAccession);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
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
                        .sequence(sq("AAAAA"))
                        .build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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
        UniProtKBEntry entry =
                new UniProtKBEntryBuilder(
                                "P12345", "ACCESSION_" + species, UniProtKBEntryType.TREMBL)
                        .sequence(sq("AAAAA"))
                        .build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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
                        .sequence(sq("AAAAA"))
                        .build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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

        Map<String, SuggestDocument> suggestions = new HashMap<>();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, suggestions);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(
                Arrays.asList("KW-1111", "keyword value", "KW-9994", "Domain"), document.keywords);

        // check suggestions
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
    void convertEncodedInFields() {
        // given
        GeneLocation geneLocation =
                new GeneLocationBuilder()
                        .geneEncodingType(GeneEncodingType.CYANELLE)
                        .value("geneLocation value")
                        .evidencesAdd(createEvidence("60"))
                        .build();

        UniProtKBEntry entry = getBasicEntryBuilder().geneLocationsAdd(geneLocation).build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(Arrays.asList("plastid", "cyanelle"), document.encodedIn);
        assertEquals(1, document.id.size());
        assertTrue(document.id.contains("UNIPROT_ENTRYID"));
    }

    @Test
    void convertProteinExistenceFields() {
        // given
        UniProtKBEntry entry =
                getBasicEntryBuilder().proteinExistence(ProteinExistence.PROTEIN_LEVEL).build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(5, document.seqLength);
        assertEquals(373, document.seqMass);
    }

    @Test
    void convertEntryScore() {
        // given
        UniProtKBEntry entry = getBasicEntryBuilder().sequence(sq("AAAAA")).build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
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

        Evidence pdbEvidence =
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
                                        .evidencesAdd(pdbEvidence)
                                        .build())
                        .build();
        UniProtKBEntry entry = getBasicEntryBuilder().genesAdd(gene).build();

        // when
        UniProtEntryConverter converter =
                new UniProtEntryConverter(null, null, null, null, null, null);
        UniProtDocument document = converter.convert(entry);

        // then
        assertEquals("P12345", document.accession);
        assertEquals(4, document.sources.size());
        assertTrue(document.sources.contains("hamapid"));
        assertTrue(document.sources.contains("hamap"));
        assertTrue(document.sources.contains("pdbid"));
        assertTrue(document.sources.contains("pdb"));
    }

    private UniProtKBEntryBuilder getBasicEntryBuilder() {
        return new UniProtKBEntryBuilder("P12345", "UNIPROT_ENTRYID", UniProtKBEntryType.SWISSPROT)
                .sequence(sq("AAAAA"));
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

    private Sequence sq(String seq) {
        return new SequenceBuilder(seq).build();
    }
}
