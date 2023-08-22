package org.uniprot.store.spark.indexer.proteome.mapper;

import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.*;
import org.uniprot.core.proteome.impl.*;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.uniprot.core.proteome.CPDStatus.STANDARD;
import static org.uniprot.core.proteome.ProteomeType.*;

class ProteomeEntryToProteomeDocumentMapperTest {

    public static final String ID = "ID";
    public static final String STRAIN = "strain";
    public static final String COMPONENT_0 = "component0";
    public static final String COMPONENT_1 = "component1";
    public static final String COMPONENT_2 = "component2";
    public static final String GENOMIC_SOURCE_0 = "source0";
    public static final String GENOMIC_SOURCE_1 = "source1";
    public static final String GENOMIC_SOURCE_2 = "source2";
    public static final String GENOME_ASSEMBLY_ID = "genomeAssemblyId";
    public static final int PROTEIN_COUNT_0 = 23;
    public static final int PROTEIN_COUNT_1 = 99;
    public static final int ANNOTATION_SCORE = 479;
    private static final CPDStatus cPDStatus = STANDARD;
    public static final ProteomeType PROTEOME_TYPE = NORMAL;
    public static final int BUSCO_COMPLETE = 39;
    public static final int BUSCO_TOTAL = 82;
    public static final int TAXON_ID = 123;
    private final GenomeAnnotation genomicAnnotation0 = new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_0).build();
    private final GenomeAnnotation genomicAnnotation1 = new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_1).build();
    private final GenomeAnnotation genomicAnnotation2 = new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_2).build();
    private final Component component0 = new ComponentBuilder().name(COMPONENT_0).genomeAnnotation(genomicAnnotation0).proteinCount(PROTEIN_COUNT_0).build();
    private final Component component1 = new ComponentBuilder().name(COMPONENT_1).genomeAnnotation(genomicAnnotation1).proteinCount(PROTEIN_COUNT_1).build();
    private final Component component2 = new ComponentBuilder().name(COMPONENT_2).genomeAnnotation(genomicAnnotation2).build();
    private final BuscoReport buscoReport = new BuscoReportBuilder().complete(BUSCO_COMPLETE).total(BUSCO_TOTAL).build();
    private final CPDReport cpdReport = new CPDReportBuilder().status(cPDStatus).build();
    private final ProteomeCompletenessReport proteomeCompletenessReport = new ProteomeCompletenessReportBuilder().buscoReport(buscoReport).cpdReport(cpdReport).build();
    private final GenomeAssembly genomeAssembly = new GenomeAssemblyBuilder().assemblyId(GENOME_ASSEMBLY_ID).build();
    ;
    private final ProteomeEntryToProteomeDocumentMapper proteomeEntryToProteomeDocumentMapper = new ProteomeEntryToProteomeDocumentMapper();
    private ProteomeEntryBuilder proteomeEntryBuilder;

    @BeforeEach
    void setUp() {
        proteomeEntryBuilder = new ProteomeEntryBuilder().proteomeId(ID)
                .taxonomy(new TaxonomyBuilder().taxonId(TAXON_ID).build())
                .strain(STRAIN)
                .componentsSet(List.of(component0, component1, component2))
                .genomeAssembly(genomeAssembly)
                .annotationScore(ANNOTATION_SCORE)
                .proteomeCompletenessReport(proteomeCompletenessReport)
                .proteomeType(PROTEOME_TYPE);
    }

    @Test
    void call() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(2, false, false, false),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    private void assertProteomeDocument(ProteomeDocument proteomeDocument, ProteomeTypeInfo proteomeTypeInfo, List<String> genomeAssembly, float busco) {
        assertSame(ID, proteomeDocument.upid);
        assertEquals(TAXON_ID, proteomeDocument.organismTaxId);
        assertSame(STRAIN, proteomeDocument.strain);
        assertEquals(ANNOTATION_SCORE, proteomeDocument.score);
        assertThat(proteomeDocument.genomeAccession, contains(GENOMIC_SOURCE_0, GENOMIC_SOURCE_1, GENOMIC_SOURCE_2));
        assertEquals(proteomeDocument.genomeAssembly, genomeAssembly);
        assertEquals(proteomeDocument.proteinCount, PROTEIN_COUNT_0 + PROTEIN_COUNT_1);
        assertEquals(proteomeDocument.busco, busco);
        assertSame(proteomeDocument.cpd, cPDStatus.getId());
        assertEquals(proteomeTypeInfo.proteomeType, proteomeDocument.proteomeType);
        assertEquals(proteomeTypeInfo.isReferenceProteome, proteomeDocument.isReferenceProteome);
        assertEquals(proteomeTypeInfo.isExcluded, proteomeDocument.isExcluded);
        assertEquals(proteomeTypeInfo.isRedundant, proteomeDocument.isRedundant);
    }

    @Test
    void call_whenBuscoTotalIsZero() throws Exception {
        BuscoReport buscoReport = new BuscoReportBuilder().complete(0).total(0).build();
        ProteomeCompletenessReport proteomeCompletenessReport = new ProteomeCompletenessReportBuilder().buscoReport(buscoReport).cpdReport(cpdReport).build();
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeCompletenessReport(proteomeCompletenessReport).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(2, false, false, false),
                List.of(GENOME_ASSEMBLY_ID), 0f);
    }

    @Test
    void call_whenGenomeAssemblyNull() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.genomeAssembly(null).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(2, false, false, false),
                List.of(), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Test
    void call_whenProteomeTypeReference() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REFERENCE).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Test
    void call_whenProteomeTypeRepresentative() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REPRESENTATIVE).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Test
    void call_whenProteomeTypeReferenceAndRepresentative() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REFERENCE_AND_REPRESENTATIVE).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Test
    void call_whenProteomeTypeExcluded() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(EXCLUDED).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(4, false, true, false),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Test
    void call_whenProteomeTypeRedundant() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REDUNDANT).build();
        ProteomeDocument proteomeDocument = proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);
        assertProteomeDocument(proteomeDocument, new ProteomeTypeInfo(3, false, false, true),
                List.of(GENOME_ASSEMBLY_ID), (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL);
    }

    @Data
    private static class ProteomeTypeInfo {
        final int proteomeType;
        final boolean isReferenceProteome;
        final boolean isExcluded;
        final boolean isRedundant;
    }

}