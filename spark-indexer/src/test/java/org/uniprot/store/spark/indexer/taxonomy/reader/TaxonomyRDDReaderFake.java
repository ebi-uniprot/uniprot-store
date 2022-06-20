package org.uniprot.store.spark.indexer.taxonomy.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;

import scala.Tuple2;

public class TaxonomyRDDReaderFake extends TaxonomyRDDReader {

    private final JobParameter jobParameter;
    private final boolean includeOrganism;

    public TaxonomyRDDReaderFake(
            JobParameter jobParameter, boolean withLineage, boolean includeOrganism) {
        super(jobParameter, withLineage);
        this.jobParameter = jobParameter;
        this.includeOrganism = includeOrganism;
    }

    @Override
    public JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
        List<Tuple2<String, List<TaxonomyLineage>>> lineage = new ArrayList<>();
        lineage.add(new Tuple2<>("10116", lineages(10116, 10114, 39107, 10066)));
        lineage.add(new Tuple2<>("10114", lineages(10114, 39107, 10066)));
        lineage.add(new Tuple2<>("39107", lineages(39107, 10066)));
        lineage.add(new Tuple2<>("10066", lineages(10066)));

        lineage.add(new Tuple2<>("289376", lineages(289376, 289375)));
        lineage.add(new Tuple2<>("289375", lineages(289375)));

        lineage.add(new Tuple2<>("11049", lineages(11049)));
        lineage.add(new Tuple2<>("60714", lineages(60714, 60713)));
        lineage.add(new Tuple2<>("1076255", lineages(1076255, 1076254)));
        lineage.add(new Tuple2<>("1559365", lineages(1559365, 1559364)));
        lineage.add(new Tuple2<>("337687", lineages(337687)));

        return jobParameter.getSparkContext().parallelizePairs(lineage);
    }

    private List<TaxonomyLineage> lineages(int... taxonIds) {
        List<TaxonomyLineage> lineages = new ArrayList<>();
        int i = 1;
        if (includeOrganism) {
            i = 0;
        }

        for (; i < taxonIds.length; i++) {
            int taxonId = taxonIds[i];
            lineages.add(taxonomyLineage(taxonId));
        }
        return lineages;
    }

    private TaxonomyLineage taxonomyLineage(int taxonId) {
        return new TaxonomyLineageBuilder()
                .taxonId(taxonId)
                .scientificName("scientificName for " + taxonId)
                .commonName("commonName for " + taxonId)
                .rank(TaxonomyRank.FAMILY)
                .build();
    }
}
// 10116 —> 10114 —> 39107 —>10066 —>  1
