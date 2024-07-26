package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;

import com.typesafe.config.Config;

import scala.Tuple2;

public class UniParcCrossReferenceDataStoreIndexerTest {
    @Test
    void indexInDataStore() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            UniParcCrossReferenceDataStoreIndexerTest.FakeUniParcCrossRefDataStoreIndexer indexer =
                    new UniParcCrossReferenceDataStoreIndexerTest
                            .FakeUniParcCrossRefDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
            DataStoreParameter dataStoreParams =
                    indexer.getDataStoreParameter(parameter.getApplicationConfig());
            assertNotNull(dataStoreParams);
        }
    }

    private static class FakeUniParcCrossRefDataStoreIndexer
            extends UniParcCrossReferenceDataStoreIndexer {

        private final JobParameter jobParameter;

        public FakeUniParcCrossRefDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
            this.jobParameter = jobParameter;
        }

        @Override
        void saveInDataStore(JavaRDD<UniParcCrossReferenceWrapper> uniParcCrossRefWrap) {
            List<UniParcCrossReferenceWrapper> result = uniParcCrossRefWrap.collect();
            assertNotNull(result);
            assertEquals(22, result.size());
            assertEquals(
                    List.of(
                            "UPI00000E8551-SWISSPROT-Q9EPI6",
                            "UPI00000E8551-SWISSPROT_VARSPLIC-Q9EPI6-1",
                            "UPI00000E8551-TREMBL-Q9EPI6",
                            "UPI00000E8551-TREMBL-I8FBX0",
                            "UPI00000E8551-TREMBL-I8FBX2",
                            "UPI00000E8551-REFSEQ-NP_476538",
                            "UPI00000E8551-EMBL-CAC20866",
                            "UPI00000E8551-EMBL-CAC20866-1",
                            "UPI00000E8551-IPI-IPI00199691",
                            "UPI00000E8551-IPI-IPI00199691-1",
                            "UPI00000E8551-IPI-IPI00199691-2",
                            "UPI00000E8551-IPI-IPI00199692",
                            "UPI000000017F-SWISSPROT-O68891",
                            "UPI000000017F-TREMBL-Q71US8",
                            "UPI000000017F-TREMBL-O68891",
                            "UPI000000017F-TREMBL-I8FBX1",
                            "UPI000000017F-TREMBL-Q00007",
                            "UPI000000017F-EMBL-AAC13493",
                            "UPI000000017F-EMBL-AAC13494",
                            "UPI000000017F-JPO-DJ891176",
                            "UPI000000017F-TREMBLNEW-AAC13493",
                            "UPI000000017F-JPO-DJ891176-1"),
                    result.stream()
                            .map(UniParcCrossReferenceWrapper::getId)
                            .collect(Collectors.toList()));
        }

        @Override
        JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
            List<Tuple2<String, TaxonomyEntry>> tuple2List = new ArrayList<>();
            TaxonomyEntry tax =
                    new TaxonomyEntryBuilder().taxonId(337687).scientificName("sn337687").build();
            tuple2List.add(new Tuple2<>("337687", tax));

            TaxonomyLineage lineage =
                    new TaxonomyLineageBuilder().taxonId(100).scientificName("lineageSC").build();
            tax =
                    new TaxonomyEntryBuilder()
                            .taxonId(10116)
                            .scientificName("sn10116")
                            .lineagesAdd(lineage)
                            .build();
            tuple2List.add(new Tuple2<>("10116", tax));

            return jobParameter.getSparkContext().parallelizePairs(tuple2List);
        }
    }
}
