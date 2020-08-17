package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * @author lgonzales
 * @since 2020-02-16
 */
class UniParcDocumentConverterTest {

    @Test
    void convertSwissProt() {
        UniParcEntry entry = getUniParcEntry(UniParcDatabase.SWISSPROT);
        UniParcDocumentConverter converter = new UniParcDocumentConverter();
        UniParcDocument result = converter.convert(entry);

        assertNotNull(result.getUniprotAccessions());
        assertEquals(1, result.getUniprotAccessions().size());
        assertTrue(result.getUniprotAccessions().contains("UniProtKB/Swiss-ProtIdValue-true"));

        assertNotNull(result.getUniprotIsoforms());
        assertEquals(1, result.getUniprotIsoforms().size());
        assertTrue(result.getUniprotIsoforms().contains("UniProtKB/Swiss-ProtIdValue-true"));

        assertNotNull(result.getDatabases());
        assertEquals(2, result.getDatabases().size());
        assertTrue(result.getDatabases().contains("UniProtKB/Swiss-Prot"));
        assertTrue(result.getDatabases().contains("EMBL"));

        assertNotNull(result.getDbIds());
        assertEquals(3, result.getDbIds().size());
        assertTrue(result.getDbIds().contains("UniProtKB/Swiss-ProtIdValue-true"));
        assertTrue(result.getDbIds().contains("UniProtKB/Swiss-ProtIdValue-false"));
        assertTrue(result.getDbIds().contains("inactiveIdValue"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("UniProtKB/Swiss-Prot"));

        validateDocumentCommonValues(result);
    }

    @Test
    void convertIsoForm() {
        UniParcEntry entry = getUniParcEntry(UniParcDatabase.SWISSPROT_VARSPLIC);
        UniParcDocumentConverter converter = new UniParcDocumentConverter();
        UniParcDocument result = converter.convert(entry);

        assertNotNull(result);

        assertNotNull(result.getUniprotAccessions());
        assertTrue(result.getUniprotAccessions().isEmpty());

        assertNotNull(result.getUniprotIsoforms());
        assertEquals(1, result.getUniprotIsoforms().size());
        assertTrue(
                result.getUniprotIsoforms()
                        .contains("UniProtKB/Swiss-Prot protein isoformsIdValue-true"));

        assertNotNull(result.getDatabases());
        assertEquals(2, result.getDatabases().size());
        assertTrue(result.getDatabases().contains("UniProtKB/Swiss-Prot protein isoforms"));
        assertTrue(result.getDatabases().contains("EMBL"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("UniProtKB/Swiss-Prot protein isoforms"));

        assertNotNull(result.getDbIds());
        assertEquals(3, result.getDbIds().size());
        assertTrue(result.getDbIds().contains("UniProtKB/Swiss-Prot protein isoformsIdValue-true"));
        assertTrue(
                result.getDbIds().contains("UniProtKB/Swiss-Prot protein isoformsIdValue-false"));
        assertTrue(result.getDbIds().contains("inactiveIdValue"));

        validateDocumentCommonValues(result);
    }

    private void validateDocumentCommonValues(UniParcDocument result) {
        assertNotNull(result);
        assertTrue(result.getUpi().startsWith("uniParcIdValue"));

        assertNotNull(result.getGeneNames());
        assertEquals(1, result.getGeneNames().size());
        assertTrue(result.getGeneNames().contains("geneNameValue"));

        assertNotNull(result.getProteinNames());
        assertEquals(1, result.getProteinNames().size());
        assertTrue(result.getProteinNames().contains("proteinNameValue"));

        assertNotNull(result.getUpids());
        assertEquals(1, result.getUpids().size());
        assertTrue(result.getUpids().contains("proteomeIdValue"));

        assertNotNull(result.getOrganismTaxons());
        assertTrue(result.getOrganismTaxons().isEmpty());

        assertNotNull(result.getTaxLineageIds());
        assertEquals(1, result.getTaxLineageIds().size());
        assertTrue(result.getTaxLineageIds().contains(10));

        assertEquals("62C549AB5E41E99D", result.getSequenceChecksum());
        assertEquals(22, result.getSeqLength());

        assertTrue(result.getContent().contains(result.getUpi()));
        assertTrue(result.getContent().containsAll(result.getDatabases()));
        assertTrue(result.getContent().containsAll(result.getDbIds()));
        assertTrue(result.getContent().containsAll(result.getActives()));
        assertTrue(result.getContent().containsAll(result.getGeneNames()));
        assertTrue(result.getContent().containsAll(result.getProteinNames()));
        assertTrue(result.getContent().containsAll(result.getUpids()));
        assertTrue(result.getContent().containsAll(result.getUniprotAccessions()));
        assertTrue(result.getContent().containsAll(result.getUniprotIsoforms()));
    }

    private UniParcEntry getUniParcEntry(UniParcDatabase type) {
        return new UniParcEntryBuilder()
                .uniParcId("uniParcIdValue" + UUID.randomUUID().toString())
                .uniprotExclusionReason("")
                .uniParcCrossReferencesAdd(getDatabaseCrossReferences(type))
                .uniParcCrossReferencesAdd(getDatabaseCrossReferences(type, false))
                .uniParcCrossReferencesAdd(getInactiveDatabaseCrossReferences())
                .sequence(getSequence())
                .taxonomiesAdd(getTaxonomy())
                .sequenceFeaturesAdd(getSequenceFeatures())
                .build();
    }

    private SequenceFeature getSequenceFeatures() {
        return new SequenceFeatureBuilder().signatureDbId("signatureDbIdValue").build();
    }

    private UniParcCrossReference getDatabaseCrossReferences(UniParcDatabase type) {
        return getDatabaseCrossReferences(type, true);
    }

    private UniParcCrossReference getDatabaseCrossReferences(UniParcDatabase type, boolean active) {
        return new UniParcCrossReferenceBuilder()
                .id(type.getName() + "IdValue-" + active)
                .database(type)
                .propertiesAdd(UniParcCrossReference.PROPERTY_GENE_NAME, "geneNameValue")
                .propertiesAdd(UniParcCrossReference.PROPERTY_PROTEIN_NAME, "proteinNameValue")
                .propertiesAdd(UniParcCrossReference.PROPERTY_PROTEOME_ID, "proteomeIdValue")
                .active(active)
                .build();
    }

    private UniParcCrossReference getInactiveDatabaseCrossReferences() {
        return new UniParcCrossReferenceBuilder()
                .id("inactiveIdValue")
                .database(UniParcDatabase.EMBL)
                .version(99)
                .versionI(199)
                .active(false)
                .build();
    }

    private Taxonomy getTaxonomy() {
        return new TaxonomyBuilder()
                .taxonId(10L)
                .commonName("commonNameValue")
                .scientificName("scientificNameValue")
                .build();
    }

    private Sequence getSequence() {
        return new SequenceBuilder("MVSWGRFICLVVVTMATLSLAR").build();
    }
}
