package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.getDbReferenceSchema;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.getUniParcXMLSchema;
import static org.uniprot.store.spark.indexer.uniparc.converter.UniParcConverterUtils.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntryLight;

import scala.collection.JavaConverters;
import scala.collection.Seq;

class DatasetUniParcEntryLightConverterTest {
    @Test
    void testCompleteUniParcEntryLight() throws Exception {
        Row completeUniParcRow = getFullUniParcRow();
        DatasetUniParcEntryLightConverter converter = new DatasetUniParcEntryLightConverter();
        UniParcEntryLight entry = converter.call(completeUniParcRow);
        assertNotNull(entry);
        assertNotNull(entry.getUniParcId());
        assertFalse(entry.getUniProtKBAccessions().isEmpty());
        assertNotNull(entry.getUniParcCrossReferences());
        assertTrue(entry.getOrganisms().isEmpty());
        assertTrue(entry.getProteinNames().isEmpty());
        assertTrue(entry.getProteomeIds().isEmpty());
        assertTrue(entry.getGeneNames().isEmpty());
    }

    private Row getFullUniParcRow() {
        List<Object> entryValues = new ArrayList<>();
        entryValues.add("datasetValue"); // _dataset
        entryValues.add("UniProtKBExclusionValue"); // _UniProtKB_exclusion
        entryValues.add("accessionValue"); // accession
        entryValues.add(getDbReferenceSeq()); // dbReferences
        entryValues.add(getSequenceRow()); // sequence
        entryValues.add(getSignatureSequenceMatchSeq()); // signatureSequenceMatch
        return new GenericRowWithSchema(entryValues.toArray(), getUniParcXMLSchema());
    }

    private static Seq getDbReferenceSeq() {
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
            dbReferences.add("Y"); // _active
            dbReferences.add(11L + i); // _version
            dbReferences.add("2001-06-18"); // _created
            dbReferences.add("2020-02-16"); // _last
            dbReferences.add(getPropertiesSeq());

            Row dbReferenceRow =
                    new GenericRowWithSchema(dbReferences.toArray(), getDbReferenceSchema());
            dbReferenceSeq.add(dbReferenceRow);
        }

        return JavaConverters.asScalaIteratorConverter(dbReferenceSeq.iterator()).asScala().toSeq();
    }
}
