package org.uniprot.store.spark.indexer.chebi.mapper;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

public class ChebiNodeEntryRelatedFieldsRowMapper implements FlatMapFunction<Row, Tuple2<String, Map<String, Seq<String>>>> {
  @Override
  public Iterator<Tuple2<String, Map<String, Seq<String>>>> call(Row row) throws Exception {
    List<Tuple2<String, Map<String, Seq<String>>>> result = new ArrayList<>();
    String aboutSubject = row.getString(0);
    Map<String, Seq<String>> objectMap = JavaConverters.mapAsJavaMap(row.getMap(2));
    for (Map.Entry<String, Seq<String>> entry : objectMap.entrySet()) {
      HashMap<String, Seq<String>> valueMap = new HashMap<>();
      valueMap.put(entry.getKey(), entry.getValue());
      result.add(new Tuple2<>(aboutSubject, valueMap));
    }
    return result.iterator();
  }
}
