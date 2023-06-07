package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.uniprot.store.spark.indexer.chebi.ChebiOwlReader;

import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.uniprot.store.indexer.common.utils.Constants.*;

public class ChebiEntryRowMapper implements Function<Row, Row> {

    private final Set<String> unwantedAboutValues =
            new HashSet<>(
                    Arrays.asList(
                            "http://purl.obolibrary.org/obo/BFO_0000051",
                            "http://purl.obolibrary.org/obo/BFO_0000050",
                            "http://purl.obolibrary.org/obo/RO_0000087",
                            "http://purl.obolibrary.org/obo/chebi#has_parent_hydride",
                            "http://purl.obolibrary.org/obo/chebi#has_functional_parent",
                            "http://purl.obolibrary.org/obo/chebi#is_conjugate_acid_of",
                            "http://purl.obolibrary.org/obo/chebi#is_conjugate_base_of",
                            "http://purl.obolibrary.org/obo/chebi#is_enantiomer_of",
                            "http://purl.obolibrary.org/obo/chebi#is_substituent_group_from",
                            "http://purl.obolibrary.org/obo/chebi#is_tautomer_of",
                            "http://purl.obolibrary.org/obo/IAO_0000115",
                            "http://purl.obolibrary.org/obo/IAO_0000231",
                            "http://purl.obolibrary.org/obo/IAO_0100001",
                            "http://purl.obolibrary.org/obo/chebi/charge",
                            "http://purl.obolibrary.org/obo/chebi/formula",
                            "http://purl.obolibrary.org/obo/chebi/inchi",
                            "http://purl.obolibrary.org/obo/chebi/inchikey",
                            "http://purl.obolibrary.org/obo/chebi/mass",
                            "http://purl.obolibrary.org/obo/chebi/monoisotopicmass",
                            "http://purl.obolibrary.org/obo/chebi/smiles",
                            "http://www.geneontology.org/formats/oboInOwl#hasDbXref",
                            "http://www.geneontology.org/formats/oboInOwl#hasId"));

    @Override
    public Row call(Row row) throws Exception {
        String currentSubject = null;
        Row respRow = null;
        Map<String, Seq<String>> processedAttributes = new LinkedHashMap<>();
        String aboutValue = row.getString(row.fieldIndex(CHEBI_RDF_ABOUT_ATTRIBUTE));
        if (!unwantedAboutValues.contains(aboutValue)) {
            currentSubject = aboutValue;
        }
        if (currentSubject != null) {
            for (String key : ChebiOwlReader.getSchema().fieldNames()) {
                if (!key.equals(CHEBI_RDF_ABOUT_ATTRIBUTE) && !key.equals(CHEBI_RDF_NODE_ID_ATTRIBBUTE)) {
                    List<String> values = null;
                    if (key.equals(CHEBI_RDF_CHEBI_STRUCTURE_ATTRIBUTE) || key.equals(CHEBI_RDFS_SUBCLASS_ATTRIBUTE)) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            values =
                                    getChebiKeyValuesForRelatedAbstractSeqResourceObj(
                                            (AbstractSeq<Row>) resourceObj);
                        } else if (resourceObj instanceof String) {
                            values = getChebiKeyValuesFromStringResourceObj(row);
                        }
                    } else {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        values = getChebiValuesForNonRelatedResourceObj(resourceObj);
                    }
                    if (values != null) {
                        processedAttributes.put(key, JavaConverters.asScalaBuffer(values));
                    }
                }
            }
            respRow =
                    RowFactory.create(
                            currentSubject, JavaConverters.mapAsScalaMap(processedAttributes));
        }
        return respRow;
    }

    private List<String> getChebiValuesForNonRelatedResourceObj(Object resourceObj) {
        List<String> values = null;
        if (resourceObj instanceof String) {
            String stringValue = (String) resourceObj;
            if (!stringValue.isEmpty()) {
                values = Collections.singletonList(stringValue);
            }
        } else if (resourceObj instanceof List) {
            List<String> valueList = (List<String>) resourceObj;
            if (valueList != null && !valueList.isEmpty()) {
                values = valueList;
            }
        }
        return values;
    }

    private List<String> getChebiKeyValuesFromStringResourceObj(Row row) {
        List<String> values;
        String resourceId = null;
        String nodeId = null;
        if (Arrays.asList(row.schema().fieldNames()).contains(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
            resourceId = row.getString(row.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
        }
        if (Arrays.asList(row.schema().fieldNames()).contains(CHEBI_RDF_NODE_ID_ATTRIBBUTE)) {
            nodeId = row.getString(row.fieldIndex(CHEBI_RDF_NODE_ID_ATTRIBBUTE));
        }
        String value = resourceId != null ? resourceId : nodeId;
        values = Collections.singletonList(value);
        return values;
    }

    private List<String> getChebiKeyValuesForRelatedAbstractSeqResourceObj(
            AbstractSeq<Row> resourceObj) {
        List<String> values;
        AbstractSeq<Row> resourceArraySeq = resourceObj;
        List<Row> resourceList = JavaConverters.seqAsJavaList(resourceArraySeq);
        values =
                resourceList.stream()
                        .map(
                                resourceRow -> {
                                    String resourceId = null;
                                    String nodeId = null;
                                    if (Arrays.asList(resourceRow.schema().fieldNames())
                                            .contains(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
                                        resourceId =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
                                    }
                                    if (Arrays.asList(resourceRow.schema().fieldNames())
                                            .contains(CHEBI_RDF_NODE_ID_ATTRIBBUTE)) {
                                        nodeId =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex(CHEBI_RDF_NODE_ID_ATTRIBBUTE));
                                    }
                                    return resourceId != null ? resourceId : nodeId;
                                })
                        .collect(Collectors.toList());
        return values;
    }
}
