package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Location;
import org.uniprot.core.Property;
import org.uniprot.core.uniparc.*;
import org.uniprot.store.spark.indexer.util.RowUtils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author lgonzales
 * @since 2020-02-16
 */
class DatasetUniparcEntryConverterTest {

    @Test
    void testCompleteUniparcEntry() throws Exception {
        Row completeUniparcRow = getFullUniParcRow();
        DatasetUniparcEntryConverter converter = new DatasetUniparcEntryConverter();
        UniParcEntry entry = converter.call(completeUniparcRow);
        assertNotNull(entry);

        assertNotNull(entry.getUniParcId());
        assertEquals("accessionValue", entry.getUniParcId().getValue());
        assertEquals("UniProtKBExclusionValue", entry.getUniProtExclusionReason());

        assertNotNull(entry.getDbXReferences());
        assertEquals(1, entry.getDbXReferences().size());
        UniParcDBCrossReference dbReference = entry.getDbXReferences().get(0);
        validateDbReference(dbReference);

        assertNotNull(entry.getSequence());
        validateSequence(entry);

        assertNotNull(entry.getSequenceFeatures());
        assertEquals(1, entry.getSequenceFeatures().size());
        SequenceFeature sequenceFeature = entry.getSequenceFeatures().get(0);
        validateSequenceFeature(sequenceFeature);
    }

    @Test
    void testIncompleteUniparcEntry() throws Exception {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add(null); // _dataset
        entryValues.add(null); // _UniProtKB_exclusion
        entryValues.add("accessionValue"); // accession
        entryValues.add(null); // dbReferences
        entryValues.add(getSequenceRow()); // sequence
        entryValues.add(null); // signatureSequenceMatch

        Row incompleteUniparcRow =
                new GenericRowWithSchema(
                        entryValues.toArray(), DatasetUniparcEntryConverter.getUniParcXMLSchema());
        DatasetUniparcEntryConverter converter = new DatasetUniparcEntryConverter();
        UniParcEntry entry = converter.call(incompleteUniparcRow);
        assertNotNull(entry);

        assertNotNull(entry.getUniParcId());
        assertEquals("accessionValue", entry.getUniParcId().getValue());
        assertNull(entry.getUniProtExclusionReason());

        assertNotNull(entry.getDbXReferences());
        assertTrue(entry.getDbXReferences().isEmpty());

        assertNotNull(entry.getSequence());
        validateSequence(entry);

        assertNotNull(entry.getSequenceFeatures());
        assertTrue(entry.getSequenceFeatures().isEmpty());
    }

    private void validateSequenceFeature(SequenceFeature sequenceFeature) {
        assertNotNull(sequenceFeature);
        assertEquals("idValue", sequenceFeature.getSignatureDbId());
        assertEquals(SignatureDbType.PFAM, sequenceFeature.getSignatureDbType());

        InterProGroup group = sequenceFeature.getInterProDomain();
        assertNotNull(group);
        assertEquals("idValue", group.getId());
        assertEquals("nameValue", group.getName());

        assertNotNull(sequenceFeature.getLocations());
        assertEquals(1, sequenceFeature.getLocations().size());

        Location location = sequenceFeature.getLocations().get(0);
        assertNotNull(location);
        assertEquals(10, location.getStart());
        assertEquals(20, location.getEnd());
    }

    private void validateDbReference(UniParcDBCrossReference dbReference) {
        assertNotNull(dbReference);
        assertEquals("idValue", dbReference.getId());
        assertEquals(UniParcDatabaseType.REFSEQ, dbReference.getDatabaseType());
        assertEquals(10, dbReference.getVersionI());
        assertEquals(11, dbReference.getVersion());
        assertTrue(dbReference.isActive());
        assertEquals("2001-06-18", dbReference.getCreated().toString());
        assertEquals("2020-02-16", dbReference.getLastUpdated().toString());

        assertNotNull(dbReference.getProperties());
        assertEquals(8, dbReference.getProperties().size());

        Property property = dbReference.getProperties().get(0);
        assertEquals(UniParcDBCrossReference.PROPERTY_UNIPROT_KB_ACCESSION, property.getKey());
        assertEquals("accessionIdValue", property.getValue());
    }

    private Row getFullUniParcRow() {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("datasetValue"); // _dataset
        entryValues.add("UniProtKBExclusionValue"); // _UniProtKB_exclusion
        entryValues.add("accessionValue"); // accession
        entryValues.add(getDbReferenceSeq()); // dbReferences
        entryValues.add(getSequenceRow()); // sequence
        entryValues.add(getSignatureSequenceMatchSeq()); // signatureSequenceMatch

        return new GenericRowWithSchema(
                entryValues.toArray(), DatasetUniparcEntryConverter.getUniParcXMLSchema());
    }

    private Seq getDbReferenceSeq() {
        List<Object> dbReferences = new ArrayList<>();
        dbReferences.add("idValue"); // _id
        dbReferences.add(UniParcDatabaseType.REFSEQ.toDisplayName()); // _type
        dbReferences.add(10L); // _version_i
        dbReferences.add("Y"); // _active
        dbReferences.add(11L); // _version
        dbReferences.add("2001-06-18"); // _created
        dbReferences.add("2020-02-16"); // _last
        dbReferences.add(getPropertiesSeq()); // property

        Row dbReferenceRow =
                new GenericRowWithSchema(
                        dbReferences.toArray(),
                        DatasetUniparcEntryConverter.getDbReferenceSchema());
        List<Object> dbReferenceSeq = new ArrayList<>();
        dbReferenceSeq.add(dbReferenceRow);

        return (Seq)
                JavaConverters.asScalaIteratorConverter(dbReferenceSeq.iterator())
                        .asScala()
                        .toSeq();
    }

    private void validateSequence(UniParcEntry entry) {
        assertEquals("MVSWGRFICLVVVTMATLSLAR", entry.getSequence().getValue());
        assertEquals(22, entry.getSequence().getLength());
        assertEquals("62C549AB5E41E99D", entry.getSequence().getCrc64());
    }

    private Seq getPropertiesSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(
                getPropertyRow(
                        UniParcDBCrossReference.PROPERTY_UNIPROT_KB_ACCESSION, "accessionIdValue"));
        properties.add(getPropertyRow(UniParcDBCrossReference.PROPERTY_NCBI_TAXONOMY_ID, "100"));
        properties.add(getPropertyRow(UniParcDBCrossReference.PROPERTY_GENE_NAME, "geneNameValue"));
        properties.add(
                getPropertyRow(UniParcDBCrossReference.PROPERTY_PROTEIN_NAME, "proteinNameValue"));
        properties.add(
                getPropertyRow(UniParcDBCrossReference.PROPERTY_PROTEOME_ID, "proteomeIdValue"));
        properties.add(
                getPropertyRow(UniParcDBCrossReference.PROPERTY_COMPONENT, "componentValue"));
        properties.add(getPropertyRow(UniParcDBCrossReference.PROPERTY_CHAIN, "chainValue"));
        properties.add(getPropertyRow(UniParcDBCrossReference.PROPERTY_NCBI_GI, "ncbiGiValue"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    private Row getPropertyRow(String name, Object value) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(name);
        propertyValues.add(value);
        return new GenericRowWithSchema(propertyValues.toArray(), RowUtils.getPropertySchema());
    }

    private Row getSequenceRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add("MVSWGRFICLVVVTMATLSLAR");
        sequenceValues.add("6CD5001C960ED82F");
        sequenceValues.add("821");
        return new GenericRowWithSchema(sequenceValues.toArray(), RowUtils.getSequenceSchema());
    }

    private Seq getSignatureSequenceMatchSeq() {
        List<Object> signatureSequences = new ArrayList<>();
        signatureSequences.add("idValue"); // _id
        signatureSequences.add(SignatureDbType.PFAM.toDisplayName()); // _database
        signatureSequences.add(getFeatureGroupRow()); // ipr
        signatureSequences.add(getLocationSeq()); // lcn

        Row signatureSequenceRow =
                new GenericRowWithSchema(
                        signatureSequences.toArray(),
                        DatasetUniparcEntryConverter.getSignatureSchema());

        List<Object> signatureSequenceSeq = new ArrayList<>();
        signatureSequenceSeq.add(signatureSequenceRow);

        return (Seq)
                JavaConverters.asScalaIteratorConverter(signatureSequenceSeq.iterator())
                        .asScala()
                        .toSeq();
    }

    private Row getFeatureGroupRow() {
        List<Object> featureGroup = new ArrayList<>();
        featureGroup.add("idValue"); // _id
        featureGroup.add("nameValue"); // _name
        return new GenericRowWithSchema(
                featureGroup.toArray(), DatasetUniparcEntryConverter.getSeqFeatureGroupSchema());
    }

    private Seq getLocationSeq() {
        List<Object> location = new ArrayList<>();
        location.add(10L); // _start
        location.add(20L); // _end

        Row locationRow =
                new GenericRowWithSchema(
                        location.toArray(), DatasetUniparcEntryConverter.getLocationSchema());

        List<Object> locationRowSeq = new ArrayList<>();
        locationRowSeq.add(locationRow);

        return (Seq)
                JavaConverters.asScalaIteratorConverter(locationRowSeq.iterator())
                        .asScala()
                        .toSeq();
    }
}
