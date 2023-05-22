package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.uniprot.store.spark.indexer.chebi.ChebiOwlReader;

import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiNodeEntryRowMapper implements FlatMapFunction<Row, Row> {
    @Override
    public Iterator<Row> call(Row row) throws Exception {
        List<String> commonTypeValue = new ArrayList<>();
        String currentSubject = row.getString(row.fieldIndex("_rdf:nodeID"));
        if (currentSubject != null) {
            Map<String, Seq<String>> processedAttributes = new LinkedHashMap<>();
            for (String key : ChebiOwlReader.getSchema().fieldNames()) {
                if (!key.equals("_rdf:about") && !key.equals("_rdf:nodeID")) {
                    List<String> values = null;
                    if (key.equals("rdf:type")) {
                        Object resourceObj = row.get(row.fieldIndex(key));
                        if (resourceObj instanceof AbstractSeq) {
                            commonTypeValue = getNodeTypeValueForChebiEntryName(resourceObj);
                        }
                    }
                    if (key.equals("rdfs:label")) {
                        if (commonTypeValue.size() > 0) {
                            values = row.getList(row.fieldIndex(key));
                            processedAttributes.put("name", JavaConverters.asScalaBuffer(values));
                            values = null;
                            commonTypeValue = new ArrayList<>();
                        } else {
                            values = row.getList(row.fieldIndex(key));
                        }
                    } else if (key.equals("owl:onProperty") || key.equals("owl:someValuesFrom")) {
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
            Row newRow =
                    RowFactory.create(
                            currentSubject, JavaConverters.mapAsScalaMap(processedAttributes));
            return Collections.singletonList(newRow).iterator();
        }
        return Collections.emptyIterator();
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
                                            .contains("_rdf:resource")) {
                                        resourceId =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex("_rdf:resource"));
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
                                            .contains("_rdf:resource")) {
                                        String type =
                                                resourceRow.getString(
                                                        resourceRow.fieldIndex("_rdf:resource"));
                                        if (type.contains("ChEBI_Common_Name")) {
                                            commonType =
                                                    resourceRow.getString(
                                                            resourceRow.fieldIndex(
                                                                    "_rdf:resource"));
                                        }
                                    }
                                    return commonType;
                                })
                        .collect(Collectors.toList());
        commonTypeValue.removeIf(String::isEmpty);
        return commonTypeValue;
    }
}
