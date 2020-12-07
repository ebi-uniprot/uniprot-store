package org.uniprot.store.reader.publications;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.MappedSource;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Created 07/12/2020
 *
 * @author Edd
 */
class AbstractMappedReferenceConverterTest {
    @Test
    void convertsCorrectly() {
        FakeMappedReferenceConverter mapper = new FakeMappedReferenceConverter();
        FakeMappedReference mappedReference =
                mapper.convert(
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");
        assertThat(mappedReference.acc.getValue(), is("Q1MDE9"));
        assertThat(mappedReference.pubmed, is("19597156"));
        assertThat(
                mappedReference.getSources(),
                hasItems(
                        new MappedSourceBuilder()
                                .source("ORCID")
                                .sourceIdsAdd("0000-0002-4251-0362")
                                .build()));
        assertThat(mappedReference.cats, hasItems("Function", "Pathology & Biotech"));
    }

    @Test
    void invalidLineFormatCausesException() {
        FakeMappedReferenceConverter mapper = new FakeMappedReferenceConverter();
        assertThrows(
                RawMappedReferenceException.class,
                () ->
                        mapper.convert(
                                "ORCID195971560000-0002-4251-0362\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas."));
    }

    @Test
    void emptyUniProtAccessionCausesException() {
        FakeMappedReferenceConverter mapper = new FakeMappedReferenceConverter();
        assertThrows(
                RawMappedReferenceException.class,
                () ->
                        mapper.convert(
                                "\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas."));
    }

    @Test
    void emptyReferenceCausesException() {
        FakeMappedReferenceConverter mapper = new FakeMappedReferenceConverter();
        assertThrows(
                RawMappedReferenceException.class,
                () ->
                        mapper.convert(
                                "Q1MDE9\tORCID\t\t0000-0002-4251-0362\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas."));
    }

    private static class FakeMappedReferenceConverter
            extends AbstractMappedReferenceConverter<FakeMappedReference> {
        FakeMappedReference convertRawMappedReference(RawMappedReference reference) {
            FakeMappedReference mappedReference = new FakeMappedReference();
            mappedReference.acc = new UniProtKBAccessionBuilder(reference.accession).build();
            mappedReference.cats = reference.categories;
            mappedReference.pubmed = reference.pubMedId;
            HashSet<MappedSource> sources = new HashSet<>();
            sources.add(
                    new MappedSourceBuilder()
                            .source(reference.source)
                            .sourceIdsAdd(reference.sourceId)
                            .build());
            mappedReference.sources = sources;
            return mappedReference;
        }
    }

    private static class FakeMappedReference implements MappedReference {
        Set<String> cats;
        UniProtKBAccession acc;
        Set<MappedSource> sources;
        String pubmed;

        @Override
        public UniProtKBAccession getUniProtKBAccession() {
            return acc;
        }

        @Override
        public Set<MappedSource> getSources() {
            return sources;
        }

        @Override
        public String getPubMedId() {
            return pubmed;
        }

        @Override
        public Set<String> getSourceCategories() {
            return cats;
        }
    }
}
