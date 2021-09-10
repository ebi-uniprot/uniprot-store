package org.uniprot.store.indexer.proteome;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.uniprot.core.xml.proteome.ScoreBuscoConverter.*;
import static org.uniprot.core.xml.proteome.ScoreCPDConverter.*;

import java.io.File;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.uniprot.core.proteome.CPDStatus;
import org.uniprot.core.proteome.ExclusionReason;
import org.uniprot.core.xml.jaxb.proteome.*;
import org.uniprot.core.xml.proteome.ScoreBuscoConverter;
import org.uniprot.core.xml.proteome.ScoreCPDConverter;
import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author lgonzales
 * @since 09/10/2020
 */
@ExtendWith(SpringExtension.class)
@TestPropertySource(locations = "classpath:application.properties")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProteomeDocumentConverterTest {

    @Value(("${uniprotkb.indexing.taxonomyFile}"))
    private String taxonomyFile;

    private TaxonomyMapRepo taxonomyRepo;

    @BeforeAll
    void setupTaxonomyRepo() {
        taxonomyRepo = new TaxonomyMapRepo(new FileNodeIterable(new File(taxonomyFile)));
    }

    @Test
    void convertExcludedChangeIsExcludedTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        ExclusionType exclusionType = xmlFactory.createExclusionType();
        exclusionType.getExclusionReason().add(ExclusionReason.MIXED_CULTURE.getName());
        proteome.setExcluded(exclusionType);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertTrue(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(4, result.proteomeType);
    }

    @Test
    void convertReferenceChangeIsReferenceTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        proteome.setIsReferenceProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertTrue(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(1, result.proteomeType);
    }

    @Test
    void convertRepresentativeChangeIsReferenceTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        proteome.setIsRepresentativeProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertTrue(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(1, result.proteomeType);
    }

    @Test
    void convertReferenceAndRepresentativeChangeIsReferenceTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        proteome.setIsRepresentativeProteome(true);
        proteome.setIsReferenceProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertTrue(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(1, result.proteomeType);
    }

    @Test
    void convertRedundantChangeIsRedundantTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        proteome.setRedundantTo("UP123457");

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertTrue(result.isRedundant);
        assertEquals(3, result.proteomeType);
    }

    @Test
    void convertCompleteProteome() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setDescription("Proteome Description");
        proteome.setStrain("Strain value");
        proteome.setTaxonomy(289376L);

        GenomeAnnotationType genomeAnnotation = xmlFactory.createGenomeAnnotationType();
        genomeAnnotation.setGenomeAnnotationSource("GASource");
        genomeAnnotation.setGenomeAnnotationUrl("GAUrl");

        ComponentType component = xmlFactory.createComponentType();
        component.setName("component Name");
        component.setProteinCount(10);
        component.getGenomeAccession().add("P21802");
        component.setBiosampleId("GCSetAccValue");
        component.setGenomeAnnotation(genomeAnnotation);
        proteome.getComponent().add(component);

        AnnotationScoreType annotationScore = xmlFactory.createAnnotationScoreType();
        annotationScore.setNormalizedAnnotationScore(2);
        proteome.setAnnotationScore(annotationScore);

        GenomeAssemblyType genomeAssembly = xmlFactory.createGenomeAssemblyType();
        genomeAssembly.setGenomeAssembly("GAValue");
        genomeAssembly.setGenomeAssemblyUrl("GAUrl");
        genomeAssembly.setGenomeAssemblySource("EnsemblMetazoa");
        genomeAssembly.setGenomeRepresentation("full");
        proteome.setGenomeAssembly(genomeAssembly);
        proteome.getScores().add(getBuscoScore());
        proteome.getScores().add(getCPDScore());
        ProteomeDocumentConverter converter = new ProteomeDocumentConverter(taxonomyRepo);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(2, result.proteomeType);

        assertNotNull(result.organismName);
        assertEquals(2, result.organismName.size());
        assertTrue(
                result.organismName.contains(
                        "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87)"));
        assertTrue(result.organismName.contains("THEYD"));

        assertEquals(
                "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87)",
                result.organismSort);
        assertEquals(289376, result.organismTaxId);

        assertEquals(8, result.organismTaxon.size());
        assertTrue(
                result.organismTaxon.contains(
                        "Thermodesulfovibrio yellowstonii (strain ATCC 51303 / DSM 11347 / YP87)"));
        assertTrue(result.organismTaxon.contains("THEYD"));
        assertTrue(result.organismTaxon.contains("Bacteria"));
        assertTrue(result.organismTaxon.contains("cellular organisms"));

        assertNotNull(result.taxLineageIds);
        assertEquals(4, result.taxLineageIds.size());
        assertTrue(result.taxLineageIds.contains(289376));
        assertTrue(result.taxLineageIds.contains(131567));
        assertTrue(result.taxLineageIds.contains(2));

        assertEquals("Bacteria", result.superkingdom);

        assertNotNull(result.genomeAccession);
        assertEquals(1, result.genomeAccession.size());
        assertTrue(result.genomeAccession.contains("P21802"));

        assertNotNull(result.genomeAssembly);
        assertEquals(1, result.genomeAssembly.size());
        assertTrue(result.genomeAssembly.contains("GAValue"));

        assertEquals(2, result.score);
        assertEquals(1, result.cpd);
        assertEquals(75.0f, result.busco);
        assertEquals(10, result.proteinCount);

        assertEquals("Strain value", result.strain);
    }

    private ScoreType getCPDScore() {
        ObjectFactory xmlFactory = new ObjectFactory();
        ScoreType scoreType = xmlFactory.createScoreType();
        scoreType.setName(ScoreCPDConverter.NAME);
        ScorePropertyType property = createProperty(PROPERTY_AVERAGE_CDS, "10");
        scoreType.getProperty().add(property);
        property = createProperty(PROPERTY_STATUS, CPDStatus.STANDARD.getDisplayName());
        scoreType.getProperty().add(property);
        property = createProperty(PROPERTY_STD_CDSS, "14");
        scoreType.getProperty().add(property);
        return scoreType;
    }

    private ScoreType getBuscoScore() {
        ObjectFactory xmlFactory = new ObjectFactory();
        ScoreType scoreType = xmlFactory.createScoreType();
        scoreType.setName(ScoreBuscoConverter.NAME);
        ScorePropertyType property = createProperty(PROPERTY_TOTAL, "20");
        scoreType.getProperty().add(property);
        property = createProperty(PROPERTY_COMPLETED, "15");
        scoreType.getProperty().add(property);
        property = createProperty(PROPERTY_LINEAGE, "lineage value");
        scoreType.getProperty().add(property);
        return scoreType;
    }

    private ScorePropertyType createProperty(String name, String value) {
        ObjectFactory xmlFactory = new ObjectFactory();
        ScorePropertyType property = xmlFactory.createScorePropertyType();
        property.setName(name);
        property.setValue(value);
        return property;
    }
}
