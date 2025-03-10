package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.uniparc.impl.UniParcEntryLightBuilder.HAS_ACTIVE_CROSS_REF;
import static org.uniprot.store.spark.indexer.uniparc.converter.BaseUniParcEntryConverter.getDbReferenceSchema;
import static org.uniprot.store.spark.indexer.uniparc.converter.BaseUniParcEntryConverter.getUniParcXMLSchema;
import static org.uniprot.store.spark.indexer.uniparc.converter.UniParcConverterUtils.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntryLight;

import scala.collection.JavaConverters;
import scala.collection.Seq;

class DatasetUniParcEntryLightConverterTest {
    @Test
    void testCompleteUniParcEntryLight() throws Exception {
        Row completeUniParcRow = getFullUniParcRow(true);
        DatasetUniParcEntryLightConverter converter = new DatasetUniParcEntryLightConverter();
        UniParcEntryLight entry = converter.call(completeUniParcRow);
        assertNotNull(entry);
        assertNotNull(entry.getUniParcId());
        assertFalse(entry.getUniProtKBAccessions().isEmpty());
        assertEquals(10, entry.getCrossReferenceCount());
        assertNotNull(entry.getCommonTaxons());
        assertEquals(10, entry.getCommonTaxons().size());
        assertTrue(entry.getOrganisms().isEmpty());
        assertTrue(entry.getProteinNames().isEmpty());
        assertTrue(entry.getProteomes().isEmpty());
        assertTrue(entry.getGeneNames().isEmpty());
        assertTrue(entry.getExtraAttributes().isEmpty());
        SequenceFeature sequenceFeature = entry.getSequenceFeatures().get(0);
        validateSequenceFeature(sequenceFeature);
    }

    @Test
    void testCompleteUniParcEntryLightWithInactiveCrossRef() throws Exception {
        Row completeUniParcRow = getFullUniParcRow(false);
        DatasetUniParcEntryLightConverter converter = new DatasetUniParcEntryLightConverter();
        UniParcEntryLight entry = converter.call(completeUniParcRow);
        assertNotNull(entry);
        assertNotNull(entry.getUniParcId());
        assertEquals(1, entry.getExtraAttributes().size());
        assertEquals(false, entry.getExtraAttributes().get(HAS_ACTIVE_CROSS_REF));
    }

    private Row getFullUniParcRow(boolean active) {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("datasetValue"); // _dataset
        entryValues.add("UniProtKBExclusionValue"); // _UniProtKB_exclusion
        entryValues.add("accessionValue"); // accession
        entryValues.add(getDbReferenceSeq(active)); // dbReferences
        entryValues.add(getSequenceRow()); // sequence
        entryValues.add(getSignatureSequenceMatchSeq()); // signatureSequenceMatch
        return new GenericRowWithSchema(entryValues.toArray(), getUniParcXMLSchema());
    }

    private static Seq getDbReferenceSeq(boolean active) {
        List<Object> dbReferenceSeq = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            List<Object> dbReferences = new ArrayList<>();
            dbReferences.add("idValue" + i); // _id
            if (i % 3 == 0) {
                dbReferences.add(UniParcDatabase.TREMBL.getDisplayName()); // _type
            } else if (i % 4 == 0) {
                dbReferences.add(UniParcDatabase.SWISSPROT.getDisplayName()); // _type
            } else {
                dbReferences.add(UniParcDatabase.REFSEQ.getDisplayName()); // _type
            }
            dbReferences.add(10L + i); // _version_i
            dbReferences.add(active ? "Y" : "N");
            dbReferences.add(11L + i); // _version
            dbReferences.add("2001-06-18"); // _created
            dbReferences.add("2020-02-16"); // _last
            dbReferences.add(getPropertiesSeq(i));

            Row dbReferenceRow =
                    new GenericRowWithSchema(dbReferences.toArray(), getDbReferenceSchema());
            dbReferenceSeq.add(dbReferenceRow);
        }

        return JavaConverters.asScalaIteratorConverter(dbReferenceSeq.iterator()).asScala().toSeq();
    }
}
