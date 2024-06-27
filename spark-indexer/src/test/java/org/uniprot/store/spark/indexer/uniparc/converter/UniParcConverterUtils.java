package org.uniprot.store.spark.indexer.uniparc.converter;

import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.*;
import static org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter.getLocationSchema;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.uniprot.core.uniparc.SignatureDbType;
import org.uniprot.store.spark.indexer.common.util.RowUtils;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class UniParcConverterUtils {

    static Seq getPropertiesSeq() {
        return getPropertiesSeq(100);
    }

    static Seq getPropertiesSeq(int i) {
        List<Object> properties = new ArrayList<>();
        properties.add(getPropertyRow(PROPERTY_NCBI_TAXONOMY_ID, String.valueOf(i)));
        properties.add(getPropertyRow(PROPERTY_GENE_NAME, "geneNameValue"));
        properties.add(getPropertyRow(PROPERTY_PROTEIN_NAME, "proteinNameValue"));
        properties.add(getPropertyRow(PROPERTY_PROTEOME_ID, "proteomeIdValue"));
        properties.add(getPropertyRow(PROPERTY_COMPONENT, "componentValue"));
        properties.add(getPropertyRow(PROPERTY_CHAIN, "chainValue"));
        properties.add(getPropertyRow(PROPERTY_NCBI_GI, "ncbiGiValue"));
        properties.add(getPropertyRow(PROPERTY_UNIPROTKB_ACCESSION, "P12345"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    static Seq getInvalidPropertiesSeq() {
        List<Object> properties = new ArrayList<>();
        properties.add(getPropertyRow(PROPERTY_NCBI_TAXONOMY_ID, "100"));
        properties.add(getPropertyRow(PROPERTY_GENE_NAME, "geneNameValue"));
        properties.add(getPropertyRow(PROPERTY_PROTEIN_NAME, "proteinNameValue"));
        properties.add(getPropertyRow("INVALID", "INVALID"));
        return (Seq)
                JavaConverters.asScalaIteratorConverter(properties.iterator()).asScala().toSeq();
    }

    static Row getPropertyRow(String name, Object value) {
        List<Object> propertyValues = new ArrayList<>();
        propertyValues.add("_VALUE");
        propertyValues.add(name);
        propertyValues.add(value);
        return new GenericRowWithSchema(propertyValues.toArray(), RowUtils.getPropertySchema());
    }

    static Row getSequenceRow() {
        List<Object> sequenceValues = new ArrayList<>();
        sequenceValues.add("MVSWGRFICLVVVTMATLSLAR");
        sequenceValues.add("6CD5001C960ED82F");
        sequenceValues.add("821");
        return new GenericRowWithSchema(sequenceValues.toArray(), RowUtils.getSequenceSchema());
    }

    static Seq getSignatureSequenceMatchSeq() {
        List<Object> signatureSequences = new ArrayList<>();
        signatureSequences.add("idValue"); // _id
        signatureSequences.add(SignatureDbType.PFAM.getDisplayName()); // _database
        signatureSequences.add(getFeatureGroupRow()); // ipr
        signatureSequences.add(getLocationSeq()); // lcn

        Row signatureSequenceRow =
                new GenericRowWithSchema(signatureSequences.toArray(), getSignatureSchema());

        List<Object> signatureSequenceSeq = new ArrayList<>();
        signatureSequenceSeq.add(signatureSequenceRow);

        return (Seq)
                JavaConverters.asScalaIteratorConverter(signatureSequenceSeq.iterator())
                        .asScala()
                        .toSeq();
    }

    static Row getFeatureGroupRow() {
        List<Object> featureGroup = new ArrayList<>();
        featureGroup.add("idValue"); // _id
        featureGroup.add("nameValue"); // _name
        return new GenericRowWithSchema(featureGroup.toArray(), getSeqFeatureGroupSchema());
    }

    static Seq getLocationSeq() {
        List<Object> location = new ArrayList<>();
        location.add(10L); // _start
        location.add(20L); // _end

        Row locationRow = new GenericRowWithSchema(location.toArray(), getLocationSchema());

        List<Object> locationRowSeq = new ArrayList<>();
        locationRowSeq.add(locationRow);

        return (Seq)
                JavaConverters.asScalaIteratorConverter(locationRowSeq.iterator())
                        .asScala()
                        .toSeq();
    }
}
