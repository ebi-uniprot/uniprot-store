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

    public TaxonomyRDDReaderFake(JobParameter jobParameter, boolean withLineage) {
        super(jobParameter, withLineage);
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
        List<Tuple2<String, List<TaxonomyLineage>>> lineage = new ArrayList<>();
        lineage.add(new Tuple2<>("10116", lineages(10114, 39107, 10066)));
        lineage.add(new Tuple2<>("10114", lineages(39107, 10066)));
        lineage.add(new Tuple2<>("39107", lineages(10066)));

        lineage.add(new Tuple2<>("10066", lineages()));
        lineage.add(new Tuple2<>("289376", lineages(289376)));
        lineage.add(new Tuple2<>("11049", lineages(11049)));
        lineage.add(new Tuple2<>("60714", lineages(60714)));
        lineage.add(new Tuple2<>("1076255", lineages(1076255)));
        lineage.add(new Tuple2<>("1559365", lineages(1559365)));
        lineage.add(new Tuple2<>("337687", lineages()));

        return jobParameter.getSparkContext().parallelizePairs(lineage);
    }

    private List<TaxonomyLineage> lineages(int... taxonIds) {
        List<TaxonomyLineage> lineages = new ArrayList<>();
        for (int taxonId : taxonIds) {
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
