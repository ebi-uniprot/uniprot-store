package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.uniprot.store.indexer.common.utils.Constants.*;

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

public class ChebiEntryMapper implements PairFunction<Row, Long, ChebiEntry>, Serializable {

    private static final String RO_0018033 = "0018033";
    private static final String RO_0018034 = "0018034";
    private static final String RELATED_MICROSPECIES_PREFIX = "has_major_microspecies_at_pH7_3";
    public static final String CHEMROF_INCHI_KEY_STRING = "chemrof:inchi_key_string";
    public static final String CHEBI_PREFIX = "CHEBI_";
    public static final String OBO_CHEBI_PATH = "/obo/CHEBI_";
    public static final String SUBJECT = "subject";
    public static final String NAME = "name";

    @Override
    public Tuple2<Long, ChebiEntry> call(Row row) throws Exception {
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
        String id = row.getAs(SUBJECT).toString().split(OBO_CHEBI_PATH)[1];
        chebiBuilder.id(id);
        chebiBuilder.name(map.get(NAME).get(0));
        chebiBuilder.inchiKey(
                map.get(CHEMROF_INCHI_KEY_STRING) != null
                        ? map.get(CHEMROF_INCHI_KEY_STRING).get(0)
                        : "");
        if (map.get(CHEBI_RDFS_LABEL_ATTRIBUTE) != null
                && map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).size() > 0) {
            for (int i = 0; i < map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).size(); i++) {
                chebiBuilder.synonymsAdd(map.get(CHEBI_RDFS_LABEL_ATTRIBUTE).get(i));
            }
        }
        if (map.get(CHEBI_RDFS_SUBCLASS_ATTRIBUTE) != null) {
            for (int i = 0; i < map.get(CHEBI_RDFS_SUBCLASS_ATTRIBUTE).size(); i++) {
                String rdfsSubClassValue = map.get(CHEBI_RDFS_SUBCLASS_ATTRIBUTE).get(i);
                if (rdfsSubClassValue.contains(CHEBI_PREFIX)) {
                    String chebiId = rdfsSubClassValue.split(OBO_CHEBI_PATH)[1].strip();
                    ;
                    if (!containsId(relatedIds, chebiId)) {
                        relatedIds.add(chebiId);
                        chebiBuilder.relatedIdsAdd(new ChebiEntryBuilder().id(chebiId).build());
                    }
                }
            }
        }
        if (map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE) != null) {
            for (int i = 0; i < map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE).size(); i++) {
                String prop = map.get(CHEBI_OWL_PROPERTY_ATTRIBUTE).get(i);
                if (prop.contains(RO_0018033)
                        || prop.contains(RO_0018034)) {
                    String owlSomeValuesFrom =
                            (map.get(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE)
                                            .get(i)
                                            .split(OBO_CHEBI_PATH)[1])
                                    .strip();
                    if (!containsId(relatedIds, owlSomeValuesFrom)) {
                        relatedIds.add(owlSomeValuesFrom);
                        chebiBuilder.relatedIdsAdd(
                                new ChebiEntryBuilder().id(owlSomeValuesFrom).build());
                    }
                }
                if (prop.contains(RELATED_MICROSPECIES_PREFIX)) {
                    String owlSomeValuesFrom =
                            (map.get(CHEBI_OWL_PROPERTY_VALUES_ATTRIBUTE)
                                            .get(i)
                                            .split(OBO_CHEBI_PATH)[1])
                                    .strip();
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
