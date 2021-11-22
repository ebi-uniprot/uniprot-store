package org.uniprot.store.spark.indexer.uniref.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.cv.go.GoAspect;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.impl.*;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * @author lgonzales
 * @since 2020-02-10
 */
class UniRefDocumentConverterTest {

    private static final Date d22Feb2020 =
            Date.from(
                    LocalDate.of(2020, Month.FEBRUARY, 22)
                            .atStartOfDay(ZoneOffset.UTC)
                            .toInstant());

    @Test
    void testConverterWithMinimalEntry() {
        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .organismTaxId(1)
                        .organismName("organismName")
                        .sequence(new SequenceBuilder("AAAAA").build())
                        .build();

        UniRefEntry entry =
                new UniRefEntryBuilder()
                        .representativeMember(representativeMember)
                        .entryType(UniRefType.UniRef50)
                        .id("id")
                        .name("name")
                        .updated(LocalDate.now())
                        .build();

        UniRefDocumentConverter converter = new UniRefDocumentConverter();
        UniRefDocument result = converter.convert(entry);

        assertNotNull(result);
        assertEquals("id", result.getId());
        assertEquals("0.5", result.getIdentity());
        assertEquals("name", result.getName());
        assertEquals("organismName", result.getOrganismSort());
        assertTrue(result.getTaxLineageIds().contains(1));
        assertTrue(result.getOrganismTaxons().contains("organismName"));
    }

    @Test
    void testConverterWithUniParcIds() {
        UniRefDocumentConverter converter = new UniRefDocumentConverter();
        UniRefEntry entry = getUniRefEntry(UniRefMemberIdType.UNIPARC);
        UniRefDocument result = converter.convert(entry);

        assertNotNull(result);
        assertTrue(result.getUniprotIds().contains("representativeMemberAccession1"));
        assertTrue(result.getUniprotIds().contains("representativeMemberAccession2"));
        assertTrue(result.getUniprotIds().contains("memberAccession1"));
        assertTrue(result.getUniprotIds().contains("memberAccession2"));

        assertTrue(result.getUpids().contains("representativeMemberId"));
        assertTrue(result.getUpids().contains("representativeMemberUniparcId"));
        assertTrue(result.getUpids().contains("memberId"));
        assertTrue(result.getUpids().contains("memberUniparcId"));

        validateCommonDocumentFields(result);
    }

    @Test
    void testConverterWithUniProtIds() {
        UniRefDocumentConverter converter = new UniRefDocumentConverter();
        UniRefEntry entry = getUniRefEntry(UniRefMemberIdType.UNIPROTKB);
        UniRefDocument result = converter.convert(entry);

        assertNotNull(result);
        assertTrue(result.getUniprotIds().contains("representativeMemberAccession1"));
        assertTrue(result.getUniprotIds().contains("representativeMemberAccession2"));
        assertTrue(result.getUniprotIds().contains("representativeMemberId"));
        assertTrue(result.getUniprotIds().contains("memberAccession1"));
        assertTrue(result.getUniprotIds().contains("memberAccession2"));
        assertTrue(result.getUniprotIds().contains("memberId"));

        assertTrue(result.getUpids().contains("representativeMemberUniparcId"));
        assertTrue(result.getUpids().contains("memberUniparcId"));

        assertTrue(result.getClusters().contains("representativeMemberUniref50Id"));
        assertTrue(result.getClusters().contains("representativeMemberUniref90Id"));
        assertTrue(result.getClusters().contains("representativeMemberUniref100Id"));
        assertTrue(result.getClusters().contains("memberUniref50Id"));
        assertTrue(result.getClusters().contains("memberUniref90Id"));
        assertTrue(result.getClusters().contains("memberUniref100Id"));
        validateCommonDocumentFields(result);
    }

    private void validateCommonDocumentFields(UniRefDocument result) {
        assertEquals("UniRef100_id", result.getId());
        assertEquals("1.0", result.getIdentity());
        assertEquals("cluster name", result.getName());
        assertEquals(2, result.getCount());
        assertEquals(11, result.getLength());
        assertEquals(d22Feb2020, result.getCreated());
        assertEquals(
                "representativeMemberOrganismName memberorganismName", result.getOrganismSort());
        assertTrue(result.getTaxLineageIds().contains(1));
        assertTrue(result.getOrganismTaxons().contains("representativeMemberOrganismName"));
    }

    private UniRefEntry getUniRefEntry(UniRefMemberIdType type) {
        UniRefMember member = getUniRefMember(type);
        RepresentativeMember representativeMember = getRepresentativeMember(type);

        return new UniRefEntryBuilder()
                .id("UniRef100_id")
                .name("cluster name")
                .memberCount(2)
                .updated(LocalDate.of(2020, 2, 22))
                .entryType(UniRefType.UniRef100)
                .commonTaxonId(3L)
                .commonTaxon("UniRefCommonTaxon")
                .goTermsAdd(
                        new GeneOntologyEntryBuilder().aspect(GoAspect.COMPONENT).id("id").build())
                .representativeMember(representativeMember)
                .membersAdd(member)
                .build();
    }

    private RepresentativeMember getRepresentativeMember(UniRefMemberIdType type) {
        Sequence sequence = new SequenceBuilder("AAAAAAAAAAA").build();
        return new RepresentativeMemberBuilder()
                .memberIdType(type)
                .memberId("representativeMemberId")
                .organismName("representativeMemberOrganismName")
                .organismTaxId(1)
                .sequenceLength(11)
                .proteinName("representativeMemberProteinName")
                .accessionsAdd(
                        new UniProtKBAccessionBuilder("representativeMemberAccession1").build())
                .accessionsAdd(
                        new UniProtKBAccessionBuilder("representativeMemberAccession2").build())
                .uniref50Id(new UniRefEntryIdBuilder("representativeMemberUniref50Id").build())
                .uniref90Id(new UniRefEntryIdBuilder("representativeMemberUniref90Id").build())
                .uniref100Id(new UniRefEntryIdBuilder("representativeMemberUniref100Id").build())
                .uniparcId(new UniParcIdBuilder("representativeMemberUniparcId").build())
                .overlapRegion(new OverlapRegionBuilder().start(30).end(40).build())
                .isSeed(true)
                .sequence(sequence)
                .build();
    }

    private UniRefMember getUniRefMember(UniRefMemberIdType type) {
        return new UniRefMemberBuilder()
                .memberIdType(type)
                .memberId("memberId")
                .organismName("memberorganismName")
                .organismTaxId(2)
                .sequenceLength(10)
                .proteinName("memberProteinName")
                .accessionsAdd(new UniProtKBAccessionBuilder("memberAccession1").build())
                .accessionsAdd(new UniProtKBAccessionBuilder("memberAccession2").build())
                .uniref50Id(new UniRefEntryIdBuilder("memberUniref50Id").build())
                .uniref90Id(new UniRefEntryIdBuilder("memberUniref90Id").build())
                .uniref100Id(new UniRefEntryIdBuilder("memberUniref100Id").build())
                .uniparcId(new UniParcIdBuilder("memberUniparcId").build())
                .overlapRegion(new OverlapRegionBuilder().start(10).end(20).build())
                .isSeed(true)
                .build();
    }
}
