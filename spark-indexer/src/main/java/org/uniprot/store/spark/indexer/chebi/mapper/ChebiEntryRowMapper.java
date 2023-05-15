package org.uniprot.store.spark.indexer.chebi.mapper;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.uniprot.store.spark.indexer.chebi.ChebiOwlReader;
import scala.collection.AbstractSeq;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

import static org.uniprot.store.spark.indexer.chebi.ChebiOwlReader.unwantedAboutValues;

public class ChebiEntryRowMapper implements FlatMapFunction<Row, Row> {

  @Override
  public Iterator<Row> call(Row row) throws Exception {
    String currentSubject = null;
    Map<String, Seq<String>> processedAttributes = new LinkedHashMap<>();
    String aboutValue = row.getString(row.fieldIndex("_rdf:about"));
    if (!unwantedAboutValues.contains(aboutValue)) {
      currentSubject = aboutValue;
    }
    if (currentSubject != null) {
      for (String key : ChebiOwlReader.getSchema().fieldNames()) {
        if (!key.equals("_rdf:about") && !key.equals("_rdf:nodeID")) {
          List<String> values = null;
          if (key.equals("chebiStructuredName") || key.equals("rdfs:subClassOf")) {
            Object resourceObj = row.get(row.fieldIndex(key));
            if (resourceObj instanceof AbstractSeq) {
              values = getChebiKeyValuesForRelatedAbstractSeqResourceObj((AbstractSeq<Row>) resourceObj);
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
      Row newRow = RowFactory.create(currentSubject, JavaConverters.mapAsScalaMap(processedAttributes));
      return Collections.singletonList(newRow).iterator();
    }
    return Collections.emptyIterator();
  }

  private static List<String> getChebiValuesForNonRelatedResourceObj(Object resourceObj) {
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

  private static List<String> getChebiKeyValuesFromStringResourceObj(Row row) {
    List<String> values;
    String resourceId = null;
    String nodeId = null;
    if (Arrays.asList(row.schema().fieldNames()).contains("_rdf:resource")) {
      resourceId = row.getString(row.fieldIndex("_rdf:resource"));
    }
    if (Arrays.asList(row.schema().fieldNames()).contains("_rdf:nodeID")) {
      nodeId = row.getString(row.fieldIndex("_rdf:nodeID"));
    }
    String value = resourceId != null ? resourceId : nodeId;
    values = Collections.singletonList(value);
    return values;
  }

  private static List<String> getChebiKeyValuesForRelatedAbstractSeqResourceObj(AbstractSeq<Row> resourceObj) {
    List<String> values;
    AbstractSeq<Row> resourceArraySeq = resourceObj;
    List<Row> resourceList = JavaConverters.seqAsJavaList(resourceArraySeq);
    values = resourceList.stream().map(resourceRow -> {
      String resourceId = null;
      String nodeId = null;
      if (Arrays.asList(resourceRow.schema().fieldNames()).contains("_rdf:resource")) {
        resourceId = resourceRow.getString(resourceRow.fieldIndex("_rdf:resource"));
      }
      if (Arrays.asList(resourceRow.schema().fieldNames()).contains("_rdf:nodeID")) {
        nodeId = resourceRow.getString(resourceRow.fieldIndex("_rdf:nodeID"));
      }
      return resourceId != null ? resourceId : nodeId;
    }).collect(Collectors.toList());
    return values;
  }
}
