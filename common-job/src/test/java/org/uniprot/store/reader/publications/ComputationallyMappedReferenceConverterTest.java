package org.uniprot.store.reader.publications;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.core.Is.is;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.uniprot.core.publication.ComputationallyMappedReference;
import org.uniprot.core.publication.impl.MappedSourceBuilder;

/**
 * Created 03/12/2020
 *
 * @author Edd
 */
class ComputationallyMappedReferenceConverterTest {
    @Test
    void convertsCorrectly() throws IOException {
        ComputationallyMappedReferenceConverter mapper =
                new ComputationallyMappedReferenceConverter();
        ComputationallyMappedReference reference =
                mapper.convert(
                        "Q1MDE9\tORCID\t19597156\t0000-0002-4251-0362\t[Function][Disease & Variants]Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas.");

        assertThat(reference.getUniProtKBAccession().getValue(), is("Q1MDE9"));
        assertThat(
                reference.getSource(),
                is(new MappedSourceBuilder().name("ORCID").id("0000-0002-4251-0362").build()));
        assertThat(reference.getCitationId(), is("19597156"));
        assertThat(reference.getSourceCategories(), contains("Function", "Disease & Variants"));
        assertThat(
                reference.getAnnotation(),
                is(
                        "Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine, isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis with peas."));
    }

    @Test
    void convertsCorrectlyWithoutAnnotation() throws IOException {
        ComputationallyMappedReferenceConverter mapper =
                new ComputationallyMappedReferenceConverter();
        ComputationallyMappedReference reference =
                mapper.convert(
                        "P17427\tMGI\t23640057\t101920\t[Function][Subcellular Location][Phenotypes & Variants]\n");

        assertThat(reference.getUniProtKBAccession().getValue(), is("P17427"));
        assertThat(
                reference.getSource(),
                is(new MappedSourceBuilder().name("MGI").id("101920").build()));
        assertThat(reference.getCitationId(), is("23640057"));
        assertThat(
                reference.getSourceCategories(),
                contains("Function", "Phenotypes & Variants", "Subcellular Location"));
        assertThat(reference.getAnnotation(), emptyString());
    }
}
