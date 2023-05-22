package org.uniprot.store.spark.indexer.chebi.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ChebiEntryRelatedFieldsRowMapper implements FlatMapFunction<Row, Row> {
    @Override
    public Iterator<Row> call(Row row) throws Exception {
        List<Row> results = new ArrayList<>();
        String subject = row.getAs("about_subject");
        scala.collection.Map<Object, Object> objectRow = row.getMap(1);
        List<String> chebiStructuredNames =
                objectRow.contains("chebiStructuredName")
                        ? JavaConverters.seqAsJavaListConverter(
                                        (Seq<String>) objectRow.get("chebiStructuredName").get())
                                .asJava()
                        : null;
        List<String> subClassOfs =
                objectRow.contains("rdfs:subClassOf")
                        ? JavaConverters.seqAsJavaListConverter(
                                        (Seq<String>) objectRow.get("rdfs:subClassOf").get())
                                .asJava()
                        : null;
        if (chebiStructuredNames != null) {
            for (String chebiStructuredName : chebiStructuredNames) {
                results.add(RowFactory.create(subject, chebiStructuredName, null));
            }
        }
        if (subClassOfs != null) {
            for (String subClassOf : subClassOfs) {
                results.add(RowFactory.create(subject, null, subClassOf));
            }
        }
        return results.iterator();
    }
}
