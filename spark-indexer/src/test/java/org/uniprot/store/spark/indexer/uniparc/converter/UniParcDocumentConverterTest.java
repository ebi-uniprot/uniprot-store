package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceImpl;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcDBCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.builder.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.builder.UniParcDBCrossReferenceBuilder;
import org.uniprot.core.uniparc.builder.UniParcEntryBuilder;
import org.uniprot.core.uniprot.taxonomy.Taxonomy;
import org.uniprot.core.uniprot.taxonomy.builder.TaxonomyBuilder;
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
        assertTrue(result.getUniprotAccessions().contains("UniProtKB/Swiss-ProtIdValue"));

        assertNotNull(result.getUniprotIsoforms());
        assertEquals(1, result.getUniprotIsoforms().size());
        assertTrue(result.getUniprotIsoforms().contains("UniProtKB/Swiss-ProtIdValue"));

        assertNotNull(result.getDatabases());
        assertEquals(2, result.getDatabases().size());
        assertTrue(result.getDatabases().contains("UniProtKB/Swiss-Prot"));
        assertTrue(result.getDatabases().contains("EMBL"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("UniProtKB/Swiss-Prot"));

        validataDocumentCommonValues(result);
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
                        .contains("UniProtKB/Swiss-Prot protein isoformsIdValue"));

        assertNotNull(result.getDatabases());
        assertEquals(2, result.getDatabases().size());
        assertTrue(result.getDatabases().contains("UniProtKB/Swiss-Prot protein isoforms"));
        assertTrue(result.getDatabases().contains("EMBL"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("UniProtKB/Swiss-Prot protein isoforms"));

        validataDocumentCommonValues(result);
    }

    private void validataDocumentCommonValues(UniParcDocument result) {
        assertNotNull(result);
        assertEquals("uniParcIdValue", result.getUpi());

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
        assertTrue(result.getContent().containsAll(result.getActives()));
        assertTrue(result.getContent().containsAll(result.getGeneNames()));
        assertTrue(result.getContent().containsAll(result.getProteinNames()));
        assertTrue(result.getContent().containsAll(result.getUpids()));
        assertTrue(result.getContent().containsAll(result.getUniprotAccessions()));
        assertTrue(result.getContent().containsAll(result.getUniprotIsoforms()));
    }

    private UniParcEntry getUniParcEntry(UniParcDatabase type) {
        return new UniParcEntryBuilder()
                .uniParcId("uniParcIdValue")
                .uniprotExclusionReason("")
                .databaseCrossReferencesAdd(getDatabaseCrossReferences(type))
                .databaseCrossReferencesAdd(getInactiveDatabaseCrossReferences())
                .sequence(getSequence())
                .taxonomiesAdd(getTaxonomy())
                .sequenceFeaturesAdd(getSequenceFeatures())
                .build();
    }

    private SequenceFeature getSequenceFeatures() {
        return new SequenceFeatureBuilder().signatureDbId("signatureDbIdValue").build();
    }

    private UniParcDBCrossReference getDatabaseCrossReferences(UniParcDatabase type) {
        return new UniParcDBCrossReferenceBuilder()
                .id(type.getName() + "IdValue")
                .databaseType(type)
                .propertiesAdd(UniParcDBCrossReference.PROPERTY_GENE_NAME, "geneNameValue")
                .propertiesAdd(UniParcDBCrossReference.PROPERTY_PROTEIN_NAME, "proteinNameValue")
                .propertiesAdd(UniParcDBCrossReference.PROPERTY_PROTEOME_ID, "proteomeIdValue")
                .active(true)
                .build();
    }

    private UniParcDBCrossReference getInactiveDatabaseCrossReferences() {
        return new UniParcDBCrossReferenceBuilder()
                .id("inactiveIdValue")
                .databaseType(UniParcDatabase.EMBL)
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
        return new SequenceImpl("MVSWGRFICLVVVTMATLSLAR");
    }
}
