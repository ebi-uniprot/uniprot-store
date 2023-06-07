package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.chebi.ChebiOwlReader;

import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import static org.uniprot.store.indexer.common.utils.Constants.*;

public class ChebiNodeEntryRowMapper implements Function<Row, Row> {
    @Override
    public Row call(Row row) throws Exception {
        List<String> commonTypeValue = new ArrayList<>();
        Row respRow = null;
        String currentSubject = row.getString(row.fieldIndex(CHEBI_RDF_NODE_ID_ATTRIBBUTE));
        if (currentSubject != null) {
            Map<String, Seq<String>> processedAttributes = new LinkedHashMap<>();
            for (String key : ChebiOwlReader.getSchema().fieldNames()) {
                if (!key.equals(CHEBI_RDF_ABOUT_ATTRIBUTE) && !key.equals(CHEBI_RDF_NODE_ID_ATTRIBBUTE)) {
                    List<String> values = null;
                    if (key.equals(CHEBI_RDF_TYPE_ATTRIBUTE)) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            commonTypeValue = getNodeTypeValueForChebiEntryName(resourceObj);
                        } else if (resourceObj instanceof List) {
                            List<Row> valueList = (List<Row>) resourceObj;
                            if (!valueList.isEmpty()) {
                                GenericRow genericRow = (GenericRow) valueList.get(0);
                                if(genericRow.get(0).toString().equals(CHEBI_RDF_RESOURCE_ATTRIBUTE) && genericRow.get(1).toString().contains("ChEBI_Common_Name") ) {
                                    commonTypeValue.add(genericRow.get(1).toString());
                                }
                            }
                        }
                    }
                    if (key.equals(CHEBI_RDFS_LABEL_ATTRIBUTE)) {
                        if (commonTypeValue.size() > 0) {
                            values = row.getList(row.fieldIndex(key));
                            processedAttributes.put("name", JavaConverters.asScalaBuffer(values));
                            values = null;
                            commonTypeValue = new ArrayList<>();
                        } else {
                            values = row.getList(row.fieldIndex(key));
                        }
                    } else if (key.equals(CHEBI_OWL_PROPERTY_ATTRIBUTE) || key.equals(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE)) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            values =
                                    getNodeKeyValuesForRelatedAbstractSeqResourceObj(
                                            (AbstractSeq<Row>) resourceObj);
                        } else if (resourceObj instanceof List) {
                            List<Row> valueList = (List<Row>) resourceObj;
                            if (!valueList.isEmpty()) {
                                GenericRow genericRow = (GenericRow) valueList.get(0);
                                if(genericRow.get(0).toString().equals(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
                                    values = new ArrayList<>();
                                    values.add(genericRow.get(1).toString());
                                }
                            }
                        }
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

    private List<String> getNodeKeyValuesForRelatedAbstractSeqResourceObj(
            AbstractSeq<Row> resourceObj) {
        List<String> values;
        AbstractSeq<Row> resourceArraySeq = resourceObj;
        List<Row> resourceList = JavaConverters.seqAsJavaList(resourceArraySeq);
        values =
                resourceList.stream()
                        .map(
                                resourceRow -> {
                                    String resourceId = null;
                                    if (Arrays.asList(resourceRow.schema().fieldNames())
                                            .contains(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
                                        resourceId =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
                                    }
                                    return resourceId;
                                })
                        .collect(Collectors.toList());
        return values;
    }

    private List<String> getNodeTypeValueForChebiEntryName(Object resourceObj) {
        List<String> commonTypeValue = null;
        AbstractSeq<Row> resourceArraySeq = (AbstractSeq<Row>) resourceObj;
        List<Row> resourceList = JavaConverters.seqAsJavaList(resourceArraySeq);
        commonTypeValue =
                resourceList.stream()
                        .map(
                                resourceRow -> {
                                    String commonType = "";
                                    if (Arrays.asList(resourceRow.schema().fieldNames())
                                            .contains(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
                                        String type =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
                                        if (type.contains("ChEBI_Common_Name")) {
                                            commonType =
                                                    resourceRow.getString(
                                                            resourceRow.fieldIndex(
                                                                    CHEBI_RDF_RESOURCE_ATTRIBUTE));
                                        }
                                    }
                                    return commonType;
                                })
                        .filter(Utils::notNullNotEmpty)
                        .collect(Collectors.toList());
        return commonTypeValue;
    }
}
