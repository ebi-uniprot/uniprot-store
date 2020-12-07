package org.uniprot.store.reader.publications;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.impl.MappedSourceBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
/**
 * Created 03/12/2020
 *
 * @author Edd
 */
class CommunityMappedReferenceConverterTest {
    @Test
    void convertsCorrectly() {
        CommunityMappedReferenceConverter mapper = new CommunityMappedReferenceConverter();
        CommunityMappedReference reference =
                mapper.convert(
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Disease: This is a disease. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");

        assertThat(reference.getUniProtKBAccession().getValue(), is("Q1MDE9"));
        assertThat(
                reference.getSources(),
                hasItems(
                        new MappedSourceBuilder()
                                .source("ORCID")
                                .sourceIdsAdd("0000-0002-4251-0362")
                                .build()));
        assertThat(reference.getPubMedId(), is("19597156"));
        assertThat(reference.getSourceCategories(), contains("Function", "Pathology & Biotech"));

        CommunityAnnotation communityAnnotation = reference.getCommunityAnnotation();
        assertThat(communityAnnotation.getProteinOrGene(), is("BraC3; RL3540."));
        assertThat(
                communityAnnotation.getFunction(),
                is(
                        "BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate."));
        assertThat(
                communityAnnotation.getComment(),
                is(
                        "Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas."));
        assertThat(communityAnnotation.getDisease(), is("This is a disease."));
    }
}
