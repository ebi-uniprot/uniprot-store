package org.uniprot.store.reader.publications;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.impl.MappedSourceBuilder;

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
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Disease & Variants]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Disease: This is a disease. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");

        validateConvertsCorrectly(reference);
    }

    @Test
    void convertsCorrectlyWithoutSpaceAfterTheCategoryNameColon() {
        CommunityMappedReferenceConverter mapper = new CommunityMappedReferenceConverter();
        CommunityMappedReference reference =
                mapper.convert(
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Disease & Variants]Protein/gene_name:BraC3; RL3540. Function:BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Disease:This is a disease. Comments:Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");

        validateConvertsCorrectly(reference);
    }

    private static void validateConvertsCorrectly(CommunityMappedReference reference) {
        assertThat(reference.getUniProtKBAccession().getValue(), is("Q1MDE9"));
        assertThat(
                reference.getSource(),
                is(new MappedSourceBuilder().name("ORCID").id("0000-0002-4251-0362").build()));
        assertThat(reference.getCitationId(), is("19597156"));
        assertThat(reference.getSourceCategories(), contains("Function", "Disease & Variants"));

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

    @Test
    void convertingSingleWordHasNoFullStop() {
        CommunityMappedReferenceConverter mapper = new CommunityMappedReferenceConverter();
        CommunityMappedReference reference =
                mapper.convert(
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Phenotypes & Variants]Protein/gene_name: RL3540. Function: BraC3. Comments: Peas Disease: This is a disease.");

        assertThat(reference.getUniProtKBAccession().getValue(), is("Q1MDE9"));
        assertThat(
                reference.getSource(),
                is(new MappedSourceBuilder().name("ORCID").id("0000-0002-4251-0362").build()));
        assertThat(reference.getCitationId(), is("19597156"));
        assertThat(reference.getSourceCategories(), contains("Function", "Phenotypes & Variants"));

        CommunityAnnotation communityAnnotation = reference.getCommunityAnnotation();
        assertThat(communityAnnotation.getProteinOrGene(), is("RL3540"));
        assertThat(communityAnnotation.getFunction(), is("BraC3"));
        assertThat(communityAnnotation.getComment(), is("Peas"));
        assertThat(communityAnnotation.getDisease(), is("This is a disease."));
    }

    @Test
    void convertsCorrectlyMultipleAnnotationsAndMultipleCategories() {
        CommunityMappedReferenceConverter mapper = new CommunityMappedReferenceConverter();
        CommunityMappedReference reference =
                mapper.convert(
                        "COMM03\tORCID\t00000003\t0000-0002-7460-6676\t[Function][Subcellular location]Protein/gene_name: EnvP(b). Function: Fusogenic properties.\n");

        validateMultipleAnnotationsAndMultipleCategories(reference);
    }

    @Test
    void
            convertsCorrectlyMultipleAnnotationsAndMultipleCategoriesWithoutSpaceAfterTheCategoryNameColon() {
        CommunityMappedReferenceConverter mapper = new CommunityMappedReferenceConverter();
        CommunityMappedReference reference =
                mapper.convert(
                        "COMM03\tORCID\t00000003\t0000-0002-7460-6676\t[Function][Subcellular location]Protein/gene_name:EnvP(b). Function:Fusogenic properties.\n");

        validateMultipleAnnotationsAndMultipleCategories(reference);
    }

    private static void validateMultipleAnnotationsAndMultipleCategories(
            CommunityMappedReference reference) {
        assertThat(reference.getUniProtKBAccession().getValue(), is("COMM03"));
        assertThat(
                reference.getSource(),
                is(new MappedSourceBuilder().name("ORCID").id("0000-0002-7460-6676").build()));
        assertThat(reference.getCitationId(), is("00000003"));
        assertThat(reference.getSourceCategories(), contains("Function", "Subcellular Location"));
        assertThat(reference.getCommunityAnnotation().getProteinOrGene(), is("EnvP(b)"));
        assertThat(reference.getCommunityAnnotation().getFunction(), is("Fusogenic properties."));
    }
}
