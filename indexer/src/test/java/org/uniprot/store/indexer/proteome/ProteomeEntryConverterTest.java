package org.uniprot.store.indexer.proteome;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.core.proteome.ExclusionReason;
import org.uniprot.core.xml.jaxb.proteome.*;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomicNodeImpl;
import org.uniprot.store.search.document.proteome.ProteomeDocument;

/**
 * @author lgonzales
 * @since 09/10/2020
 */
class ProteomeEntryConverterTest {

    @Test
    void convertExcludedChangeIsExcludedTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setName("Proteome Description");
        ExclusionType exclusionType = xmlFactory.createExclusionType();
        exclusionType.getExclusionReason().add(ExclusionReason.MIXED_CULTURE.getName());
        proteome.setExcluded(exclusionType);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertTrue(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(5, result.proteomeType);
    }

    @Test
    void convertReferenceChangeIsReferenceTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setName("Proteome Description");
        proteome.setIsReferenceProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
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
        proteome.setName("Proteome Description");
        proteome.setIsRepresentativeProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertTrue(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(2, result.proteomeType);
    }

    @Test
    void convertReferenceAndRepresentativeChangeIsReferenceTrue() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setName("Proteome Description");
        proteome.setIsRepresentativeProteome(true);
        proteome.setIsReferenceProteome(true);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
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
        proteome.setName("Proteome Description");
        proteome.setRedundantTo("UP123457");

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertTrue(result.isRedundant);
        assertEquals(4, result.proteomeType);
    }

    @Test
    void convertCompleteProteome() {
        // when
        ObjectFactory xmlFactory = new ObjectFactory();
        Proteome proteome = xmlFactory.createProteome();
        proteome.setUpid("UP123456");
        proteome.setName("Proteome Description");
        proteome.setSuperregnum(SuperregnumType.BACTERIA);
        proteome.setTaxonomy(9606L);

        DbReferenceType assemblyRef = xmlFactory.createDbReferenceType();
        assemblyRef.setType("GCSetAcc");
        assemblyRef.setId("GCSetAccValue");
        proteome.getDbReference().add(assemblyRef);

        ComponentType component = xmlFactory.createComponentType();
        component.setType(ComponentTypeType.PRIMARY);
        component.getGenomeAccession().add("P21802");
        proteome.getComponent().add(component);

        AnnotationScoreType annotationScore = xmlFactory.createAnnotationScoreType();
        annotationScore.setNormalizedAnnotationScore(2);
        proteome.setAnnotationScore(annotationScore);

        // then
        TaxonomyRepo repoMock = mock(TaxonomyRepo.class);
        TaxonomicNode taxonomyNode =
                new TaxonomicNodeImpl.Builder(9606, "scientific")
                        .withCommonName("common Name")
                        .withSynonymName("synonym")
                        .build();
        Mockito.when(repoMock.retrieveNodeUsingTaxID(9606)).thenReturn(Optional.of(taxonomyNode));
        ProteomeEntryConverter converter = new ProteomeEntryConverter(repoMock);
        ProteomeDocument result = converter.convert(proteome);
        assertNotNull(result);
        assertEquals("UP123456", result.upid);
        assertFalse(result.isExcluded);
        assertFalse(result.isReferenceProteome);
        assertFalse(result.isRedundant);
        assertEquals(3, result.proteomeType);

        assertNotNull(result.organismName);
        assertEquals(3, result.organismName.size());
        assertTrue(result.organismName.contains("scientific"));
        assertTrue(result.organismName.contains("common Name"));
        assertTrue(result.organismName.contains("synonym"));

        assertEquals("scientific", result.organismSort);
        assertEquals(9606, result.organismTaxId);

        assertEquals(6, result.organismTaxon.size());
        assertTrue(result.organismTaxon.contains("scientific"));
        assertTrue(result.organismTaxon.contains("common Name"));
        assertTrue(result.organismTaxon.contains("synonym"));

        assertNotNull(result.taxLineageIds);
        assertEquals(2, result.taxLineageIds.size());
        assertTrue(result.taxLineageIds.contains(9606));

        assertEquals("BACTERIA", result.superkingdom);

        assertNotNull(result.genomeAccession);
        assertEquals(1, result.genomeAccession.size());
        assertTrue(result.genomeAccession.contains("P21802"));

        assertNotNull(result.genomeAssembly);
        assertEquals(1, result.genomeAssembly.size());
        assertTrue(result.genomeAssembly.contains("GCSetAccValue"));

        assertEquals(2, result.score);
    }
}
