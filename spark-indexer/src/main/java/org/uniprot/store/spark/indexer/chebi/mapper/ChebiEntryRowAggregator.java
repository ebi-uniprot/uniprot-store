package org.uniprot.store.spark.indexer.chebi.mapper;

import org.apache.spark.api.java.function.Function2;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ChebiEntryRowAggregator implements Function2<Map<String, Seq<String>>, Map<String, Seq<String>>, Map<String, Seq<String>>> {
    @Override
    public Map<String, Seq<String>> call(Map<String, Seq<String>> map1, Map<String, Seq<String>> map2) throws Exception {
        if (SparkUtils.isThereAnyNullEntry(map1, map2)) {
            map1 = SparkUtils.getNotNullEntry(map1, map2);
        } else {
            for (Map.Entry<String, Seq<String>> entry : map2.entrySet()) {
                String key = entry.getKey();
                Seq<String> value = entry.getValue();
                if (map1.containsKey(key)) {
                    List<String> combinedValue =
                            new ArrayList<>(JavaConverters.seqAsJavaList(map1.get(key)));
                    combinedValue.addAll(JavaConverters.seqAsJavaList(value));
                    map1.put(key, JavaConverters.asScalaBuffer(combinedValue).toSeq());
                } else {
                    map1.put(key, value);
                }
            }
        }
        return map1;
    }
}
