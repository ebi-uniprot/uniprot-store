package org.uniprot.store.spark.indexer.uniref.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.Date;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.builder.SequenceBuilder;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniparc.impl.UniParcIdImpl;
import org.uniprot.core.uniprot.builder.UniProtAccessionBuilder;
import org.uniprot.core.uniref.*;
import org.uniprot.core.uniref.builder.RepresentativeMemberBuilder;
import org.uniprot.core.uniref.builder.UniRefEntryBuilder;
import org.uniprot.core.uniref.builder.UniRefMemberBuilder;
import org.uniprot.core.uniref.impl.GoTermImpl;
import org.uniprot.core.uniref.impl.OverlapRegionImpl;
import org.uniprot.core.uniref.impl.UniRefEntryIdImpl;
import org.uniprot.store.search.document.uniref.UniRefDocument;

/**
 * @author lgonzales
 * @since 2020-02-10
 */
class UniRefDocumentConverterTest {

    @Test
    void testConverterWithMinimalEntry() {
        RepresentativeMember representativeMember =
                new RepresentativeMemberBuilder()
                        .organismTaxId(1)
                        .organismName("organismName")
                        .sequence(new SequenceImpl("AAAAA"))
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

        assertTrue(result.getUpis().contains("representativeMemberId"));
        assertTrue(result.getUpis().contains("representativeMemberUniparcId"));
        assertTrue(result.getUpis().contains("memberId"));
        assertTrue(result.getUpis().contains("memberUniparcId"));

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

        assertTrue(result.getUpis().contains("representativeMemberUniparcId"));
        assertTrue(result.getUpis().contains("memberUniparcId"));

        validateCommonDocumentFields(result);
    }

    private void validateCommonDocumentFields(UniRefDocument result) {
        assertEquals("UniRef100_id", result.getId());
        assertEquals("1.0", result.getIdentity());
        assertEquals("cluster name", result.getName());
        assertEquals(2, result.getCount());
        assertEquals(11, result.getLength());
        assertEquals(new Date(1582329600000L), result.getCreated());
        assertEquals(
                "representativeMemberOrganismName memberorganismName", result.getOrganismSort());
        assertTrue(result.getTaxLineageIds().contains(1));
        assertTrue(result.getOrganismTaxons().contains("representativeMemberOrganismName"));

        assertTrue(result.getContent().containsAll(result.getUpis()));
        assertTrue(result.getContent().containsAll(result.getUniprotIds()));
        assertTrue(result.getContent().contains("1"));
        assertTrue(result.getContent().containsAll(result.getOrganismTaxons()));
        assertTrue(result.getContent().contains(result.getId()));
        assertTrue(result.getContent().contains(result.getName()));
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
                .commonTaxonId(3)
                .commonTaxon("UniRefCommonTaxon")
                .goTermsAdd(new GoTermImpl(GoTermType.COMPONENT, "id"))
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
                        new UniProtAccessionBuilder("representativeMemberAccession1").build())
                .accessionsAdd(
                        new UniProtAccessionBuilder("representativeMemberAccession2").build())
                .uniref50Id(new UniRefEntryIdImpl("representativeMemberUniref50Id"))
                .uniref90Id(new UniRefEntryIdImpl("representativeMemberUniref90Id"))
                .uniref100Id(new UniRefEntryIdImpl("representativeMemberUniref100Id"))
                .uniparcId(new UniParcIdImpl("representativeMemberUniparcId"))
                .overlapRegion(new OverlapRegionImpl(30, 40))
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
                .accessionsAdd(new UniProtAccessionBuilder("memberAccession1").build())
                .accessionsAdd(new UniProtAccessionBuilder("memberAccession2").build())
                .uniref50Id(new UniRefEntryIdImpl("memberUniref50Id"))
                .uniref90Id(new UniRefEntryIdImpl("memberUniref90Id"))
                .uniref100Id(new UniRefEntryIdImpl("memberUniref100Id"))
                .uniparcId(new UniParcIdImpl("memberUniparcId"))
                .overlapRegion(new OverlapRegionImpl(10, 20))
                .isSeed(true)
                .build();
    }
}
