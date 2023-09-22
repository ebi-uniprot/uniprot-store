package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.uniprot.store.indexer.common.utils.Constants.*;
import static org.uniprot.store.spark.indexer.chebi.mapper.ChebiEntryRowMapper.getAttributeValue;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.uniprot.core.util.Utils;
import org.uniprot.store.spark.indexer.chebi.ChebiOwlReader;

import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiNodeEntryRowMapper implements Function<Row, Row> {
    @Override
    public Row call(Row row) throws Exception {
        List<String> commonTypeValue = new ArrayList<>();
        Row respRow = null;
        String currentSubject = row.getString(row.fieldIndex(CHEBI_RDF_NODE_ID_ATTRIBBUTE));
        if (currentSubject != null) {
            Map<String, Seq<String>> processedAttributes = new LinkedHashMap<>();
            for (String key : ChebiOwlReader.getSchema().fieldNames()) {
                if (!key.equals(CHEBI_RDF_ABOUT_ATTRIBUTE)
                        && !key.equals(CHEBI_RDF_NODE_ID_ATTRIBBUTE)) {
                    List<String> values = null;
                    if (key.equals(CHEBI_RDF_TYPE_ATTRIBUTE)) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            commonTypeValue = getNodeTypeValueForChebiEntryName(resourceObj);
                        }
                    }
                    if (key.equals(CHEBI_RDFS_LABEL_ATTRIBUTE)) {
                        if (commonTypeValue.size() > 0) {
                            values = getColumnValues(row, key);
                            processedAttributes.put(NAME, JavaConverters.asScalaBuffer(values));
                            values = null;
                            commonTypeValue = new ArrayList<>();
                        } else {
                            values = getColumnValues(row, key);
                        }
                    } else if (key.equals(CHEBI_OWL_PROPERTY_ATTRIBUTE)
                            || key.equals(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE)) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            values =
                                    getNodeKeyValuesForRelatedAbstractSeqResourceObj(
                                            (AbstractSeq<Row>) resourceObj);
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
                                    if (resourceRow.schema() != null) {
                                        resourceId =
                                                getAttributeValue(
                                                        resourceRow, CHEBI_RDF_RESOURCE_ATTRIBUTE);
                                    } else {
                                        resourceId =
                                                resourceRow
                                                                .get(0)
                                                                .toString()
                                                                .contains(
                                                                        CHEBI_RDF_RESOURCE_ATTRIBUTE)
                                                        ? resourceRow.get(1).toString()
                                                        : null;
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
                                    if (resourceRow.schema() != null) {
                                        commonType = getNodeTypeAttributeValue(resourceRow);
                                    } else {
                                        commonType =
                                                resourceRow
                                                                        .get(0)
                                                                        .toString()
                                                                        .contains(
                                                                                CHEBI_RDF_RESOURCE_ATTRIBUTE)
                                                                && resourceRow
                                                                        .get(1)
                                                                        .toString()
                                                                        .contains(CHEBI_COMMON_NAME)
                                                        ? resourceRow.get(1).toString()
                                                        : "";
                                    }
                                    return commonType;
                                })
                        .filter(Utils::notNullNotEmpty)
                        .collect(Collectors.toList());
        return commonTypeValue;
    }

    private static String getNodeTypeAttributeValue(Row resourceRow) {
        String commonType = "";
        if (Arrays.asList(resourceRow.schema().fieldNames())
                .contains(CHEBI_RDF_RESOURCE_ATTRIBUTE)) {
            String type =
                    resourceRow.getString(resourceRow.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
            if (type.contains(CHEBI_COMMON_NAME)) {
                commonType =
                        resourceRow.getString(resourceRow.fieldIndex(CHEBI_RDF_RESOURCE_ATTRIBUTE));
            }
        }
        return commonType;
    }

    private List<String> getColumnValues(Row row, String fieldName) {
        int idx = row.fieldIndex(fieldName);
        List<String> values = null;
        if (!row.isNullAt(idx)) {
            values = row.getList(row.fieldIndex(fieldName));
        }
        return values;
    }
}
