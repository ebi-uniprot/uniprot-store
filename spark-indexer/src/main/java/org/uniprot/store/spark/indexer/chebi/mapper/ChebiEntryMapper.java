package org.uniprot.store.spark.indexer.chebi.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;
import scala.collection.JavaConverters;

import static org.uniprot.store.indexer.common.utils.Constants.*;

public class ChebiEntryMapper implements PairFunction<Row, Long, ChebiEntry>, Serializable {

    private final String RELATED_PREFIX = "is_a CHEBI:";
    private static final String RELATED_CONJUGATE_BASE_PREFIX = "is_conjugate_base_of";
    private static final String RELATED_CONJUGATE_ACID_PREFIX = "is_conjugate_acid_of";
    private static final String RELATED_MICROSPECIES_PREFIX = "has_major_microspecies_at_pH_7_3";

    @Override
    public Tuple2<Long, ChebiEntry> call(Row row) throws Exception {
        String relatedIdString = "";
        List<String> relatedIds = new ArrayList<>();
        List<String> majorMicrospecies = new ArrayList<>();
        ChebiEntryBuilder chebiBuilder = new ChebiEntryBuilder();
        scala.collection.Map<Object, Object> rawScalaMap = row.getMap(1);
        if (rawScalaMap == null) {
            return null;
        }
        Map<String, List<String>> map =
                JavaConverters.mapAsJavaMapConverter(rawScalaMap).asJava().entrySet().stream()
                        .filter(e -> e.getKey() != null && e.getValue() != null)
                        .collect(
                                Collectors.toMap(
                                        e -> (String) e.getKey(),
                                        e ->
                                                JavaConverters.seqAsJavaListConverter(
                                                                (scala.collection.Seq<String>)
                                                                        e.getValue())
                                                        .asJava()));
        String id = row.getAs("subject").toString().split("/obo/CHEBI_")[1];
        chebiBuilder.id(id);
        chebiBuilder.name(map.get("name").get(0));
        chebiBuilder.inchiKey(
                map.get("chebislash:inchikey") != null
                        ? map.get("chebislash:inchikey").get(0)
                        : "");
        if (map.get(CHEBI_RDFS_LABEL_ATTRIBUTE) != null && map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).size() > 0) {
            for (int i = 0; i < map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).size(); i++) {
                chebiBuilder.synonymsAdd(map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).get(i));
            }
        }
        if (map.get("obo:IAO_0000115") != null) {
            relatedIdString = map.get("obo:IAO_0000115").get(0);
            if (relatedIdString.startsWith(RELATED_PREFIX)) {
                String idString = relatedIdString.substring(RELATED_PREFIX.length()).strip();
                String[] ids = idString.split(RELATED_PREFIX);
                for (String idValue : ids) {
                    String cleanValue = idValue.strip().split(" ")[0];
                    if (!containsId(relatedIds, cleanValue)) {
                        relatedIds.add(cleanValue);
                        chebiBuilder.relatedIdsAdd(new ChebiEntryBuilder().id(cleanValue).build());
                    }
                }
            }
        }
        if (map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE) != null) {
            for (int i = 0; i < map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE).size(); i++) {
                String prop = map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE).get(i);
                if (prop.contains(RELATED_CONJUGATE_BASE_PREFIX)
                        || prop.contains(RELATED_CONJUGATE_ACID_PREFIX)) {
                    String owlSomeValuesFrom =
                            (map.get(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE).get(i).split("/obo/CHEBI_")[1]).strip();
                    if (!containsId(relatedIds, owlSomeValuesFrom)) {
                        relatedIds.add(owlSomeValuesFrom);
                        chebiBuilder.relatedIdsAdd(
                                new ChebiEntryBuilder().id(owlSomeValuesFrom).build());
                    }
                }
                if (prop.contains(RELATED_MICROSPECIES_PREFIX)) {
                    String owlSomeValuesFrom =
                            (map.get(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE).get(i).split("/obo/CHEBI_")[1]).strip();
                    if (!containsId(majorMicrospecies, owlSomeValuesFrom)) {
                        majorMicrospecies.add(owlSomeValuesFrom);
                        chebiBuilder.majorMicrospeciesAdd(
                                new ChebiEntryBuilder().id(owlSomeValuesFrom).build());
                    }
                }
            }
        }
        return new Tuple2<>(Long.valueOf(id), chebiBuilder.build());
    }

    public boolean containsId(List<String> idList, String id) {
        return idList.contains(id);
    }
}
