package org.uniprot.store.spark.indexer.proteome;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyH2Utils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;

import scala.Tuple2;

import com.typesafe.config.Config;

class ProteomeDocumentsToHPSWriterIT {
    private static final String RELEASE_NAME = "2020_02";
    private Connection dbConnection;

    @BeforeEach
    void setUp() throws Exception {
        Config application = SparkUtils.loadApplicationProperty();
        String url = application.getString("database.url");
        String user = application.getString("database.user.name");
        String password = application.getString("database.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        fillDatabase();
    }

    private void fillDatabase() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.createTables(statement);
        TaxonomyH2Utils.insertData(statement);
    }

    @Test
    void writeIndexDocumentsToHPS() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter jobParameter =
                    JobParameter.builder()
                            .sparkContext(sparkContext)
                            .applicationConfig(application)
                            .releaseName(RELEASE_NAME)
                            .build();

            ProteomeDocumentsToHPSWriterFake writer =
                    new ProteomeDocumentsToHPSWriterFake(jobParameter);

            writer.writeIndexDocumentsToHPS();

            List<ProteomeDocument> savedDocuments = writer.savedDocuments;
            assertEquals(7, savedDocuments.size());
        }
    }

    private static class ProteomeDocumentsToHPSWriterFake extends ProteomeDocumentsToHPSWriter {
        private final TaxonomyRDDReader taxonomyRDDReader;
        private List<ProteomeDocument> savedDocuments;

        public ProteomeDocumentsToHPSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
            taxonomyRDDReader = new TaxonomyRDDReaderFake(jobParameter, true);
        }

        @Override
        void saveToHPS(JavaRDD<ProteomeDocument> proteomeDocumentJavaRDD) {
            this.savedDocuments = proteomeDocumentJavaRDD.collect();
        }

        @Override
        JavaPairRDD<String, TaxonomyEntry> getTaxonomyRDD() {
            return taxonomyRDDReader.load();
        }
    }

    private static class TaxonomyRDDReaderFake extends TaxonomyRDDReader {
        private final JobParameter jobParameter;

        public TaxonomyRDDReaderFake(JobParameter jobParameter, boolean withLineage) {
            super(jobParameter, withLineage);
            this.jobParameter = jobParameter;
        }

        @Override
        public JavaPairRDD<String, List<TaxonomyLineage>> loadTaxonomyLineage() {
            List<Tuple2<String, List<TaxonomyLineage>>> lineage = new ArrayList<>();
            lineage.add(new Tuple2<>("10116", lineages(10066, 39107, 10114, 10116)));
            lineage.add(new Tuple2<>("10114", lineages(10066, 39107, 10114)));
            lineage.add(new Tuple2<>("39107", lineages(10066, 39107)));
            lineage.add(new Tuple2<>("10066", lineages(10066)));

            lineage.add(new Tuple2<>("289376", lineages(289375, 289376)));
            lineage.add(new Tuple2<>("289375", lineages(289375)));

            lineage.add(new Tuple2<>("11049", lineages(11049)));
            lineage.add(new Tuple2<>("60714", lineages(60713, 60714)));
            lineage.add(new Tuple2<>("1076255", lineages(1076254, 1076255)));
            lineage.add(new Tuple2<>("1559365", lineages(1559364, 1559365)));
            lineage.add(new Tuple2<>("337687", lineages(337687)));

            return jobParameter.getSparkContext().parallelizePairs(lineage);
        }

        private List<TaxonomyLineage> lineages(int... taxonIds) {
            List<TaxonomyLineage> lineages = new ArrayList<>();
            int finalId = taxonIds.length - 1;
            /*if (includeOrganism) {*/
            finalId = taxonIds.length;
            /*}*/

            for (int i = 0; i < finalId; i++) {
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

    @AfterEach
    public void teardown() throws SQLException, IOException {
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.dropTables(statement);
        dbConnection.close();
    }
}
