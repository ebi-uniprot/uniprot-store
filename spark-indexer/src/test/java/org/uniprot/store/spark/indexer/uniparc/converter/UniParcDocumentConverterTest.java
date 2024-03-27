package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Sequence;
import org.uniprot.core.impl.SequenceBuilder;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.InterProGroupBuilder;
import org.uniprot.core.uniparc.impl.SequenceFeatureBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
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
        assertTrue(result.getDatabases().contains("UniProt"));
        assertTrue(result.getDatabases().contains("embl-cds"));

        assertNotNull(result.getDatabasesFacets());
        assertEquals(2, result.getDatabasesFacets().size());
        assertTrue(result.getDatabasesFacets().contains(UniParcDatabase.SWISSPROT.getIndex()));
        assertTrue(result.getDatabasesFacets().contains(UniParcDatabase.EMBL.getIndex()));

        assertNotNull(result.getDbIds());
        assertEquals(3, result.getDbIds().size());
        assertTrue(result.getDbIds().contains("UniProtKB/Swiss-ProtIdValue-true.1"));
        assertTrue(result.getDbIds().contains("UniProtKB/Swiss-ProtIdValue-false.1"));
        assertTrue(result.getDbIds().contains("inactiveIdValue.99"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("UniProt"));

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

        assertNotNull(result.getDatabasesFacets());
        assertEquals(2, result.getDatabasesFacets().size());
        assertTrue(
                result.getDatabasesFacets()
                        .contains(UniParcDatabase.SWISSPROT_VARSPLIC.getIndex()));
        assertTrue(result.getDatabasesFacets().contains(UniParcDatabase.EMBL.getIndex()));

        assertNotNull(result.getDatabases());
        assertEquals(2, result.getDatabases().size());
        assertTrue(result.getDatabases().contains("isoforms"));
        assertTrue(result.getDatabases().contains("embl-cds"));

        assertNotNull(result.getActives());
        assertEquals(1, result.getActives().size());
        assertTrue(result.getActives().contains("isoforms"));

        assertNotNull(result.getDbIds());
        assertEquals(3, result.getDbIds().size());
        assertTrue(
                result.getDbIds().contains("UniProtKB/Swiss-Prot protein isoformsIdValue-true.1"));
        assertTrue(
                result.getDbIds().contains("UniProtKB/Swiss-Prot protein isoformsIdValue-false.1"));
        assertTrue(result.getDbIds().contains("inactiveIdValue.99"));

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
        assertNotNull(result.getProteomeComponents());
        assertEquals(1, result.getProteomeComponents().size());
        assertTrue(result.getProteomeComponents().contains("componentValue"));

        assertNotNull(result.getOrganismTaxons());
        assertTrue(result.getOrganismTaxons().isEmpty());

        assertNotNull(result.getOrganismNames());
        assertTrue(result.getOrganismNames().isEmpty());

        assertNotNull(result.getTaxLineageIds());
        assertEquals(1, result.getTaxLineageIds().size());
        assertTrue(result.getTaxLineageIds().contains(10));

        assertEquals(
                Set.of("62C549AB5E41E99D", "4F6304DA8CC16779B3B5CCDDBC663292"),
                result.getSequenceChecksums());
        assertEquals("4F6304DA8CC16779B3B5CCDDBC663292", result.getSequenceMd5());
        assertTrue(result.getSequenceChecksums().contains(result.getSequenceMd5()));

        assertEquals(2, result.getFeatureIds().size());
        assertTrue(result.getFeatureIds().contains("signatureDbIdValue"));
        assertTrue(result.getFeatureIds().contains("interProDbId"));

        assertEquals(22, result.getSeqLength());
    }

    private UniParcEntry getUniParcEntry(UniParcDatabase type) {
        return new UniParcEntryBuilder()
                .uniParcId("uniParcIdValue" + UUID.randomUUID().toString())
                .uniprotExclusionReason("")
                .uniParcCrossReferencesAdd(getDatabaseCrossReferences(type))
                .uniParcCrossReferencesAdd(getDatabaseCrossReferences(type, false))
                .uniParcCrossReferencesAdd(getInactiveDatabaseCrossReferences())
                .sequence(getSequence())
                .sequenceFeaturesAdd(getSequenceFeatures())
                .build();
    }

    private SequenceFeature getSequenceFeatures() {
        return new SequenceFeatureBuilder()
                .interproGroup(new InterProGroupBuilder().id("interProDbId").build())
                .signatureDbId("signatureDbIdValue")
                .build();
    }

    private UniParcCrossReference getDatabaseCrossReferences(UniParcDatabase type) {
        return getDatabaseCrossReferences(type, true);
    }

    private UniParcCrossReference getDatabaseCrossReferences(UniParcDatabase type, boolean active) {
        return new UniParcCrossReferenceBuilder()
                .id(type.getName() + "IdValue-" + active)
                .version(1)
                .database(type)
                .organism(getOrganism())
                .geneName("geneNameValue")
                .proteinName("proteinNameValue")
                .proteomeId("proteomeIdValue")
                .component("componentValue")
                .chain("chainValue")
                .ncbiGi("ncbiGiValue")
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

    private Organism getOrganism() {
        return new OrganismBuilder()
                .taxonId(10L)
                .commonName("commonNameValue")
                .scientificName("scientificNameValue")
                .build();
    }

    private Sequence getSequence() {
        return new SequenceBuilder("MVSWGRFICLVVVTMATLSLAR").build();
    }
}
