package org.uniprot.store.reader.publications;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import lombok.Getter;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.MappedSource;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.core.uniprotkb.UniProtKBAccession;
import org.uniprot.core.uniprotkb.impl.UniProtKBAccessionBuilder;

/**
 * Created 07/12/2020
 *
 * @author Edd
 */
class AbstractMappedReferenceConverterTest {
    private static final String ACC_PUBMED_ORCHID_LINE_PREFIX =
            "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362";

    @Test
    void convertsCorrectly() {
        FakeMappedReferenceConverter mapper = new FakeMappedReferenceConverter();
        FakeMappedReference mappedReference =
                mapper.convert(
                        ACC_PUBMED_ORCHID_LINE_PREFIX
                                + "\t[Function][Pathology & Biotech]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.sourceCategories, hasItems("Function", "Pathology & Biotech"));
    }

    private void validateAccessionPubmedOrchidInfo(FakeMappedReference mappedReference) {
        assertThat(mappedReference.uniProtKBAccession.getValue(), is("Q1MDE9"));
        assertThat(mappedReference.pubMedId, is("19597156"));
        assertThat(
                mappedReference.getSource(),
                is(new MappedSourceBuilder().name("ORCID").id("0000-0002-4251-0362").build()));
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

    @Test
    void noCategoryDefinedResultsInNoCategoriesInjected() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.";
        String line = ACC_PUBMED_ORCHID_LINE_PREFIX + "\t" + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.getSourceCategories(), is(empty()));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    @Test
    void noCategoryDefinedButBracketsInAnnotationResultsInNoCategoriesInjected() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. [This is some text] BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.";
        String line = ACC_PUBMED_ORCHID_LINE_PREFIX + "\t" + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.getSourceCategories(), is(empty()));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    @Test
    void canInjectSingleCategory() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.";
        String line = ACC_PUBMED_ORCHID_LINE_PREFIX + "\t[Function]" + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.getSourceCategories(), contains("Function"));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    @Test
    void noCategoryUponStrangeInputFromPIROrSIB() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "The impact of SorLA on the cellular processing of amyloid precursor protein (APP) is an important component in Alzheimer's disease.";
        String line = ACC_PUBMED_ORCHID_LINE_PREFIX + "\t[rerview] " + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.getSourceCategories(), is(empty()));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    @Test
    void noCategoryNoAnnotationUponReallyStrangeInputFromPIROrSIB() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String line =
                ACC_PUBMED_ORCHID_LINE_PREFIX
                        + "\t[Pulsed electric fields inhibit tumor growth but induce myocardial injury of melanoma-bearing mice].";

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(mappedReference.getSourceCategories(), is(empty()));
        assertThat(mappedReference.getAnnotation(), is(nullValue()));
    }

    @Test
    void canInjectMultipleCategories() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.";
        String line =
                ACC_PUBMED_ORCHID_LINE_PREFIX
                        + "\t[Function][Pathology & Biotech][Something else][Names]"
                        + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(
                mappedReference.getSourceCategories(),
                containsInAnyOrder("Function", "Pathology & Biotech", "Names"));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    @Test
    void canInjectCategoriesWhenAnnotationIncludesBrackets() {
        // given
        FakeMappedReferenceConverter converter = new FakeMappedReferenceConverter();
        String annotationPart =
                "Protein/gene_name: FwdA. Function: Subunit of the Tungsten-containing formylmethanofuran dehydrogenase, which catalyzes the reversible oxidation of CO2 and methanofuran to N-formylmethanofuran. FwdA is one of the catalytic subunits. FwdA contains zinc ligands, N6-carboxylysine, and a catalytically crucial aspartate. Comment: Component of the tungsten-containing active formylmethanofuran dehydrogenase, found as a dimer or tetramer of the FwdABCDFG heterohexamer. The complex contains iron sulfur clusters [4Fe-4S] and binds tungsten.";
        String line =
                ACC_PUBMED_ORCHID_LINE_PREFIX
                        + "\t[Interaction][Structure][Something else again]"
                        + annotationPart;

        // when
        FakeMappedReference mappedReference = converter.convert(line);

        // then
        validateAccessionPubmedOrchidInfo(mappedReference);
        assertThat(
                mappedReference.getSourceCategories(),
                containsInAnyOrder("Interaction", "Structure"));
        assertThat(mappedReference.getAnnotation(), is(annotationPart));
    }

    private static class FakeMappedReferenceConverter
            extends AbstractMappedReferenceConverter<FakeMappedReference> {
        FakeMappedReference convertRawMappedReference(RawMappedReference reference) {
            FakeMappedReference mappedReference = new FakeMappedReference();
            mappedReference.uniProtKBAccession =
                    new UniProtKBAccessionBuilder(reference.accession).build();
            mappedReference.sourceCategories = reference.categories;
            mappedReference.pubMedId = reference.pubMedId;
            mappedReference.source =
                    new MappedSourceBuilder().name(reference.source).id(reference.sourceId).build();
            mappedReference.annotation = reference.annotation;
            return mappedReference;
        }
    }

    @Getter
    private static class FakeMappedReference implements MappedReference {
        String annotation;
        Set<String> sourceCategories;
        UniProtKBAccession uniProtKBAccession;
        MappedSource source;
        String pubMedId;
    }
}
