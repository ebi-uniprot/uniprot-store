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
    protected JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
        List<Tuple2<String, List<TaxonomyLineage>>> lineage = new ArrayList<>();
        List<TaxonomyLineage> lineages = new ArrayList<>();
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(10114)
                        .scientificName("scientificName for 10114")
                        .commonName("commonName for 10114")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(39107)
                        .scientificName("scientificName for 39107")
                        .commonName("commonName for 39107")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(10066)
                        .scientificName("scientificName for 10066")
                        .commonName("commonName for 10066")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineage.add(new Tuple2<>("10116", lineages));

        lineages = new ArrayList<>();
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(39107)
                        .scientificName("scientificName for 39107")
                        .commonName("commonName for 39107")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(10066)
                        .scientificName("scientificName for 10066")
                        .commonName("commonName for 10066")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineage.add(new Tuple2<>("10114", lineages));

        lineages = new ArrayList<>();
        lineages.add(
                new TaxonomyLineageBuilder()
                        .taxonId(10066)
                        .scientificName("scientificName for 10066")
                        .commonName("commonName for 10066")
                        .rank(TaxonomyRank.FAMILY)
                        .build());
        lineage.add(new Tuple2<>("39107", lineages));

        lineage.add(new Tuple2<>("10066", new ArrayList<>()));
        lineage.add(new Tuple2<>("289376", new ArrayList<>()));
        lineage.add(new Tuple2<>("337687", new ArrayList<>()));

        return jobParameter.getSparkContext().parallelizePairs(lineage);
    }
}
// 10116 —> 10114 —> 39107 —>10066 —>  1
