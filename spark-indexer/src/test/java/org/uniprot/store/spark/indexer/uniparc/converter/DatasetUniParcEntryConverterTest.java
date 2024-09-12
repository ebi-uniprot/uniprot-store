package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.*;
import static org.uniprot.store.spark.indexer.uniparc.converter.UniParcConverterUtils.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.uniparc.*;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @author lgonzales
 * @since 2020-02-16
 */
class DatasetUniParcEntryConverterTest {

    @Test
    void testCompleteUniParcEntry() throws Exception {
        Row completeUniparcRow = getFullUniParcRow(true);
        DatasetUniParcEntryConverter converter = new DatasetUniParcEntryConverter();
        UniParcEntry entry = converter.call(completeUniparcRow);
        assertNotNull(entry);

        assertNotNull(entry.getUniParcId());
        assertEquals("accessionValue", entry.getUniParcId().getValue());
        assertEquals("UniProtKBExclusionValue", entry.getUniProtExclusionReason());

        assertNotNull(entry.getUniParcCrossReferences());
        assertEquals(1, entry.getUniParcCrossReferences().size());
        UniParcCrossReference dbReference = entry.getUniParcCrossReferences().get(0);
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
                new GenericRowWithSchema(entryValues.toArray(), getUniParcXMLSchema());
        DatasetUniParcEntryConverter converter = new DatasetUniParcEntryConverter();
        UniParcEntry entry = converter.call(incompleteUniparcRow);
        assertNotNull(entry);

        assertNotNull(entry.getUniParcId());
        assertEquals("accessionValue", entry.getUniParcId().getValue());
        assertNull(entry.getUniProtExclusionReason());

        assertNotNull(entry.getUniParcCrossReferences());
        assertTrue(entry.getUniParcCrossReferences().isEmpty());

        assertNotNull(entry.getSequence());
        validateSequence(entry);

        assertNotNull(entry.getSequenceFeatures());
        assertTrue(entry.getSequenceFeatures().isEmpty());
    }

    @Test
    void testWithInvalidProperties() throws Exception {
        Row invalidProps = getFullUniParcRow(false);
        DatasetUniParcEntryConverter converter = new DatasetUniParcEntryConverter();
        assertThrows(IllegalArgumentException.class, () -> converter.call(invalidProps));
    }

    private void validateDbReference(UniParcCrossReference dbReference) {
        assertNotNull(dbReference);
        assertEquals("idValue", dbReference.getId());
        assertEquals(UniParcDatabase.REFSEQ, dbReference.getDatabase());
        assertEquals(10, dbReference.getVersionI());
        assertEquals(11, dbReference.getVersion());
        assertTrue(dbReference.isActive());
        assertEquals("2001-06-18", dbReference.getCreated().toString());
        assertEquals("2020-02-16", dbReference.getLastUpdated().toString());

        assertNotNull(dbReference.getProperties());
        assertEquals(1, dbReference.getProperties().size());
        Property property = dbReference.getProperties().get(0);
        assertEquals(PROPERTY_UNIPROTKB_ACCESSION, property.getKey());
        assertEquals("P12345", property.getValue());

        assertNotNull(dbReference.getOrganism());
        assertEquals(100L, dbReference.getOrganism().getTaxonId());

        assertEquals("geneNameValue", dbReference.getGeneName());
        assertEquals("proteinNameValue", dbReference.getProteinName());
        assertEquals("chainValue", dbReference.getChain());
        assertEquals("ncbiGiValue", dbReference.getNcbiGi());
        assertEquals("proteomeIdValue", dbReference.getProteomeId());
        assertEquals("componentValue", dbReference.getComponent());
    }

    private Row getFullUniParcRow(boolean validProps) {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("datasetValue"); // _dataset
        entryValues.add("UniProtKBExclusionValue"); // _UniProtKB_exclusion
        entryValues.add("accessionValue"); // accession
        entryValues.add(getDbReferenceSeq(validProps)); // dbReferences
        entryValues.add(getSequenceRow()); // sequence
        entryValues.add(getSignatureSequenceMatchSeq()); // signatureSequenceMatch

        return new GenericRowWithSchema(entryValues.toArray(), getUniParcXMLSchema());
    }

    private Seq getDbReferenceSeq(boolean validProps) {
        List<Object> dbReferences = new ArrayList<>();
        dbReferences.add("idValue"); // _id
        dbReferences.add(UniParcDatabase.REFSEQ.getDisplayName()); // _type
        dbReferences.add(10L); // _version_i
        dbReferences.add("Y"); // _active
        dbReferences.add(11L); // _version
        dbReferences.add("2001-06-18"); // _created
        dbReferences.add("2020-02-16"); // _last
        if (validProps) {
            dbReferences.add(getPropertiesSeq()); // property
        } else {
            dbReferences.add(getInvalidPropertiesSeq()); // property
        }

        Row dbReferenceRow =
                new GenericRowWithSchema(dbReferences.toArray(), getDbReferenceSchema());
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
}
