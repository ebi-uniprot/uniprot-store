package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.taxonomy.TaxonomyLineage;

import org.uniprot.core.util.Utils;
import scala.Tuple2;

public class TaxonomyCommonalityAggregator
        implements PairFunction<
                Tuple2<String, List<List<TaxonomyLineage>>>, String, List<Tuple2<String, String>>> {
    @Serial private static final long serialVersionUID = 4504848381168970920L;

    @Override
    public Tuple2<String, List<Tuple2<String, String>>> call(
            Tuple2<String, List<List<TaxonomyLineage>>> uniParcIdLineages) throws Exception {
        Iterable<List<TaxonomyLineage>> lineages = uniParcIdLineages._2;
        Map<String, List<List<TaxonomyLineage>>> topLevelTaxonomyLineageMap = new HashMap<>();
        // create a map with top level taxonomies as key
        for (List<TaxonomyLineage> list : lineages) {
            if (!list.isEmpty()) {
                String topLevelTaxonomy = list.get(0).getScientificName();
                topLevelTaxonomyLineageMap.putIfAbsent(topLevelTaxonomy, new ArrayList<>());
                topLevelTaxonomyLineageMap.get(topLevelTaxonomy).add(list);
            }
        }

        List<Tuple2<String, String>> commonTaxons = new ArrayList<>();
        for (Map.Entry<String, List<List<TaxonomyLineage>>> entry :
                topLevelTaxonomyLineageMap.entrySet()) {
            String commonTaxon = findLastCommonTaxonomy(entry.getValue());
            Tuple2<String, String> tuple = new Tuple2<>(entry.getKey(), commonTaxon);
            commonTaxons.add(tuple);
        }

        return new Tuple2<>(uniParcIdLineages._1, commonTaxons);
    }

    String findLastCommonTaxonomy(List<List<TaxonomyLineage>> allLineages) {
        if (Utils.nullOrEmpty(allLineages)) {
            return null;
        }

        int minLength = Integer.MAX_VALUE;

        // Find the minimum length among all lineages
        for (List<TaxonomyLineage> lineage : allLineages) {
            minLength = Math.min(minLength, lineage.size());
        }

        String lastCommonTaxon = null;

        for (int i = 0; i < minLength; i++) {
            String scientificName = allLineages.get(0).get(i).getScientificName();
            boolean allSame = true;

            // Check if all lists have the same value at index i
            for (List<TaxonomyLineage> list : allLineages) {
                if (!scientificName.equals(list.get(i).getScientificName())) {
                    allSame = false;
                    break;
                }
            }

            if (!allSame) {
                break;
            }

            lastCommonTaxon = scientificName;
        }

        return lastCommonTaxon;
    }
}
