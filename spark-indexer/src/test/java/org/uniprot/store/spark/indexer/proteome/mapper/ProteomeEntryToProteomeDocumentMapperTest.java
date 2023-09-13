package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.proteome.CPDStatus.STANDARD;
import static org.uniprot.core.proteome.ProteomeType.*;

import java.nio.ByteBuffer;
import java.util.List;

import lombok.Data;

import org.apache.commons.lang.SerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.*;
import org.uniprot.core.proteome.impl.*;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

class ProteomeEntryToProteomeDocumentMapperTest {

    private static final String ID = "ID";
    private static final String STRAIN = "strain";
    private static final String COMPONENT_NAME_0 = "componentName0";
    private static final String COMPONENT_NAME_1 = "componentName1";
    private static final String COMPONENT_NAME_2 = "componentName2";
    private static final String GENOMIC_SOURCE_0 = "source0";
    private static final String GENOMIC_SOURCE_1 = "source1";
    private static final String GENOMIC_SOURCE_2 = "source2";
    private static final String GENOME_ASSEMBLY_ID = "genomeAssemblyId";
    private static final int PROTEIN_COUNT_0 = 23;
    private static final int PROTEIN_COUNT_1 = 99;
    private static final int ANNOTATION_SCORE = 479;
    private static final CPDStatus cPDStatus = STANDARD;
    private static final ProteomeType PROTEOME_TYPE = NORMAL;
    private static final int BUSCO_COMPLETE = 39;
    private static final int BUSCO_TOTAL = 82;
    private static final long TAXON_ID_0 = 0L;
    private static final long TAXON_ID_1 = 1L;
    private static final long TAXON_ID_2 = 2L;
    private static final String SCIENTIFIC_NAME_0 = "viruses";
    private static final String SCIENTIFIC_NAME_1 = "scientificName1";
    private static final String SCIENTIFIC_NAME_2 = "scientificName2";
    private static final String COMMON_NAME_0 = "commonName0";
    private static final String COMMON_NAME_1 = "commonName1";
    private static final String COMMON_NAME_2 = "commonName2";
    private static final String MNEMONIC_0 = "mnemonic0";
    private static final String MNEMONIC_1 = "mnemonic1";
    private static final String MNEMONIC_2 = "mnemonic2";
    private static final String SYNONYM_0 = "synonym0";
    private static final String SYNONYM_1 = "synonym1";
    private static final String SYNONYM_2 = "synonym2";
    private static final List<String> SYNONYMS_0 = List.of(SYNONYM_0);
    private static final List<String> SYNONYMS_1 = List.of(SYNONYM_1);
    private static final String SY = " sy";
    private static final String M = " m";
    private static final String SPACE = " ";
    private static final TaxonomyLineage taxLineage0 =
            new TaxonomyLineageBuilder()
                    .taxonId(TAXON_ID_0)
                    .scientificName(SCIENTIFIC_NAME_0)
                    .commonName(COMMON_NAME_0)
                    .build();
    private static final TaxonomyEntry TAX_ENTRY_0 =
            new TaxonomyEntryBuilder()
                    .taxonId(TAXON_ID_0)
                    .scientificName(SCIENTIFIC_NAME_0)
                    .commonName(COMMON_NAME_0)
                    .synonymsSet(SYNONYMS_0)
                    .mnemonic(MNEMONIC_0)
                    .build();
    private static final TaxonomyEntry TAX_ENTRY_1 =
            new TaxonomyEntryBuilder()
                    .taxonId(TAXON_ID_1)
                    .scientificName(SCIENTIFIC_NAME_1)
                    .commonName(COMMON_NAME_1)
                    .synonymsSet(SYNONYMS_1)
                    .mnemonic(MNEMONIC_1)
                    .lineagesSet(List.of(taxLineage0))
                    .build();
    private static final GenomeAnnotation GENOMIC_ANNOTATION_0 =
            new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_0).build();
    private static final GenomeAnnotation GENOMIC_ANNOTATION_1 =
            new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_1).build();
    private static final GenomeAnnotation GENOMIC_ANNOTATION_2 =
            new GenomeAnnotationBuilder().source(GENOMIC_SOURCE_2).build();
    private static final Component COMPONENT_0 =
            new ComponentBuilder()
                    .name(COMPONENT_NAME_0)
                    .genomeAnnotation(GENOMIC_ANNOTATION_0)
                    .proteinCount(PROTEIN_COUNT_0)
                    .build();
    private static final Component COMPONENT_1 =
            new ComponentBuilder()
                    .name(COMPONENT_NAME_1)
                    .genomeAnnotation(GENOMIC_ANNOTATION_1)
                    .proteinCount(PROTEIN_COUNT_1)
                    .build();
    private static final Component COMPONENT_2 =
            new ComponentBuilder()
                    .name(COMPONENT_NAME_2)
                    .genomeAnnotation(GENOMIC_ANNOTATION_2)
                    .build();
    private static final BuscoReport BUSCO_REPORT =
            new BuscoReportBuilder().complete(BUSCO_COMPLETE).total(BUSCO_TOTAL).build();
    private static final CPDReport CPD_REPORT = new CPDReportBuilder().status(cPDStatus).build();
    private static final ProteomeCompletenessReport PROTEOME_COMPLETENESS_REPORT =
            new ProteomeCompletenessReportBuilder()
                    .buscoReport(BUSCO_REPORT)
                    .cpdReport(CPD_REPORT)
                    .build();
    private static final GenomeAssembly GENOME_ASSEMBLY =
            new GenomeAssemblyBuilder().assemblyId(GENOME_ASSEMBLY_ID).build();
    private final ProteomeEntryToProteomeDocumentMapper proteomeEntryToProteomeDocumentMapper =
            new ProteomeEntryToProteomeDocumentMapper();
    private ProteomeEntryBuilder proteomeEntryBuilder;

    @BeforeEach
    void setUp() {
        proteomeEntryBuilder =
                new ProteomeEntryBuilder()
                        .proteomeId(ID)
                        .taxonomy(TAX_ENTRY_1)
                        .strain(STRAIN)
                        .componentsSet(List.of(COMPONENT_0, COMPONENT_1, COMPONENT_2))
                        .genomeAssembly(GENOME_ASSEMBLY)
                        .annotationScore(ANNOTATION_SCORE)
                        .proteomeCompletenessReport(PROTEOME_COMPLETENESS_REPORT)
                        .proteomeType(PROTEOME_TYPE);
    }

    @Test
    void call() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(2, false, false, false),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    private void assertProteomeDocument(
            ProteomeDocument proteomeDocument,
            ProteomeTypeInfo proteomeTypeInfo,
            List<String> genomeAssembly,
            float busco,
            ProteomeEntry proteomeEntry) {
        assertSame(ID, proteomeDocument.upid);
        assertEquals(TAXON_ID_1, proteomeDocument.organismTaxId);
        assertSame(STRAIN, proteomeDocument.strain);
        assertEquals(ANNOTATION_SCORE, proteomeDocument.score);
        assertThat(
                proteomeDocument.genomeAccession,
                contains(GENOMIC_SOURCE_0, GENOMIC_SOURCE_1, GENOMIC_SOURCE_2));
        assertEquals(proteomeDocument.genomeAssembly, genomeAssembly);
        assertEquals(proteomeDocument.proteinCount, PROTEIN_COUNT_0 + PROTEIN_COUNT_1);
        assertEquals(proteomeDocument.busco, busco);
        assertSame(proteomeDocument.cpd, cPDStatus.getId());
        assertEquals(proteomeTypeInfo.proteomeType, proteomeDocument.proteomeType);
        assertEquals(proteomeTypeInfo.isReferenceProteome, proteomeDocument.isReferenceProteome);
        assertEquals(proteomeTypeInfo.isExcluded, proteomeDocument.isExcluded);
        assertEquals(proteomeTypeInfo.isRedundant, proteomeDocument.isRedundant);
        assertEquals(SCIENTIFIC_NAME_1 + SPACE + COMMON_NAME_1 + SY, proteomeDocument.organismSort);
        assertThat(
                proteomeDocument.organismName,
                contains(SCIENTIFIC_NAME_1, COMMON_NAME_1, SYNONYM_1, MNEMONIC_1));
        assertThat(
                proteomeDocument.organismTaxon,
                contains(
                        SCIENTIFIC_NAME_1,
                        COMMON_NAME_1,
                        SYNONYM_1,
                        MNEMONIC_1,
                        SCIENTIFIC_NAME_0,
                        COMMON_NAME_0));
        assertThat(proteomeDocument.taxLineageIds, contains((int) TAXON_ID_1, (int) TAXON_ID_0));
        assertEquals(SCIENTIFIC_NAME_0, proteomeDocument.superkingdom);
        assertEquals(
                ByteBuffer.wrap(SerializationUtils.serialize(proteomeEntry)),
                proteomeDocument.proteomeStored);
    }

    @Test
    void call_whenBuscoTotalIsZero() throws Exception {
        BuscoReport buscoReport = new BuscoReportBuilder().complete(0).total(0).build();
        ProteomeCompletenessReport proteomeCompletenessReport =
                new ProteomeCompletenessReportBuilder()
                        .buscoReport(buscoReport)
                        .cpdReport(CPD_REPORT)
                        .build();
        ProteomeEntry proteomeEntry =
                proteomeEntryBuilder.proteomeCompletenessReport(proteomeCompletenessReport).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(2, false, false, false),
                List.of(GENOME_ASSEMBLY_ID),
                0f,
                proteomeEntry);
    }

    @Test
    void call_whenGenomeAssemblyNull() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.genomeAssembly(null).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(2, false, false, false),
                List.of(),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenProteomeTypeReference() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REFERENCE).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenProteomeTypeRepresentative() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REPRESENTATIVE).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenProteomeTypeReferenceAndRepresentative() throws Exception {
        ProteomeEntry proteomeEntry =
                proteomeEntryBuilder.proteomeType(REFERENCE_AND_REPRESENTATIVE).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(1, true, false, false),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenProteomeTypeExcluded() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(EXCLUDED).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(4, false, true, false),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenProteomeTypeRedundant() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.proteomeType(REDUNDANT).build();

        ProteomeDocument proteomeDocument =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertProteomeDocument(
                proteomeDocument,
                new ProteomeTypeInfo(3, false, false, true),
                List.of(GENOME_ASSEMBLY_ID),
                (float) BUSCO_COMPLETE * 100 / BUSCO_TOTAL,
                proteomeEntry);
    }

    @Test
    void call_whenOrganismHasNoLineage() throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryBuilder.taxonomy(TAX_ENTRY_0).build();

        ProteomeDocument proteomeDocumentResult =
                proteomeEntryToProteomeDocumentMapper.call(proteomeEntry);

        assertEquals(
                SCIENTIFIC_NAME_0 + SPACE + COMMON_NAME_0 + SPACE + SYNONYM_0 + M,
                proteomeDocumentResult.organismSort);
        assertThat(
                proteomeDocumentResult.organismName,
                contains(SCIENTIFIC_NAME_0, COMMON_NAME_0, SYNONYM_0, MNEMONIC_0));
        assertThat(
                proteomeDocumentResult.organismTaxon,
                contains(SCIENTIFIC_NAME_0, COMMON_NAME_0, SYNONYM_0, MNEMONIC_0));
        assertThat(proteomeDocumentResult.taxLineageIds, contains((int) TAXON_ID_0));
        assertNull(proteomeDocumentResult.superkingdom);
    }

    @Data
    private static class ProteomeTypeInfo {
        final int proteomeType;
        final boolean isReferenceProteome;
        final boolean isExcluded;
        final boolean isRedundant;
    }
}
