package org.uniprot.store.spark.indexer.proteome;

import com.typesafe.config.Config;
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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

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
            assertThat(savedDocuments, containsInAnyOrder(samePropertyValuesAs(getDoc0(), "proteomeStored"),
                    samePropertyValuesAs(getDoc1(), "proteomeStored"), samePropertyValuesAs(getDoc2(), "proteomeStored"), samePropertyValuesAs(getDoc3(), "proteomeStored"), samePropertyValuesAs(getDoc4(), "proteomeStored"),
                    samePropertyValuesAs(getDoc5(), "proteomeStored"), samePropertyValuesAs(getDoc6(), "proteomeStored")));
        }
    }

    private ProteomeDocument getDoc0() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000000718";
        proteomeDocument.organismName = new ArrayList<>(List.of("Thermodesulfovibrio yellowstonii", "THEYD"));
        proteomeDocument.organismSort = "Thermodesulfovibrio yellowston";
        proteomeDocument.organismTaxId = 289376;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Thermodesulfovibrio yellowstonii", "THEYD",
                "scientificName for 289375", "commonName for 289375", "scientificName for 289376", "commonName for 289376"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(289376, 289375, 289376));
        proteomeDocument.strain = "ATCC 51303 / DSM 11347 / YP87";
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("CP001147"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000020985.1"));
        proteomeDocument.score = 2;
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 94.354836f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 10;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc1() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000006687";
        proteomeDocument.organismName = new ArrayList<>(List.of("Porcine reproductive and respiratory syndrome virus", "PRRSV", "PRRSL"));
        proteomeDocument.organismSort = "Porcine reproductive and respi";
        proteomeDocument.organismTaxId = 11049;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Porcine reproductive and respiratory syndrome virus", "PRRSV",
                "PRRSL", "scientificName for 11049", "commonName for 11049"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(11049, 11049));
        proteomeDocument.strain = "Lelystad";
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("M96262"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_003971765.1"));
        proteomeDocument.score = 5;
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 0.0f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 11;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc2() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000029766";
        proteomeDocument.organismName = new ArrayList<>(List.of("Galinsoga mosaic virus", "GaMV", "GAMV"));
        proteomeDocument.organismSort = "Galinsoga mosaic virus GaMV GA";
        proteomeDocument.organismTaxId = 60714;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Galinsoga mosaic virus", "GaMV", "GAMV", "scientificName for 60713",
                "commonName for 60713", "scientificName for 60714", "commonName for 60714"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(60714, 60713, 60714));
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("Y13463"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000859945.1"));
        proteomeDocument.score = 2;
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 0.0f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 12;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc3() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000164653";
        proteomeDocument.organismName = new ArrayList<>(List.of("Yokapox virus", "9POXV"));
        proteomeDocument.organismSort = "Yokapox virus 9POXV";
        proteomeDocument.organismTaxId = 1076255;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Yokapox virus", "9POXV", "scientificName for 1076254",
                "commonName for 1076254", "scientificName for 1076255", "commonName for 1076255"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(1076255, 1076254, 1076255));
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("HQ849551"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000892975.1"));
        proteomeDocument.score = 2;
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 0.0f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 13;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc4() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000029775";
        proteomeDocument.organismName = new ArrayList<>(List.of("Turnip curly top virus isolate", "TCTV", "TCTVB"));
        proteomeDocument.organismSort = "PTurnip curly top virus isolate";
        proteomeDocument.organismTaxId = 1559365;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Turnip curly top virus isolate", "TCTV", "TCTVB", "scientificName for 1559364",
                "commonName for 1559364", "scientificName for 1559365", "commonName for 1559365"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(1559365, 1559364, 1559365));
        proteomeDocument.strain = "Isolate Turnip/South Africa/B11/2006";
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("GU456685"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000887455.1"));
        proteomeDocument.score = 2;
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 0.0f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 14;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc5() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000002494";
        proteomeDocument.organismName = new ArrayList<>(List.of("Rattus norvegicus", "Rat", "RAT"));
        proteomeDocument.organismSort = "Rattus norvegicus Rat RAT";
        proteomeDocument.organismTaxId = 10116;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Rattus norvegicus", "Rat", "RAT", "scientificName for 10066",
                "commonName for 10066", "scientificName for 39107", "commonName for 39107", "scientificName for 10114", "commonName for 10114", "scientificName for 10116", "commonName for 10116"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(10116, 10066, 39107, 10114, 10116));
        proteomeDocument.strain = "Brown Norway";
        proteomeDocument.isReferenceProteome = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("AY172581", "CM000072", "CM000073", "CM000074", "CM000075", "CM000076",
                "CM000077", "CM000078", "CM000079", "CM000080", "CM000081", "CM000082", "CM000083", "CM000084", "CM000085", "CM000086",
                "CM000087", "CM000088", "CM000089", "CM000090", "CM000091", "CM000092", "CM002824"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000001895.4"));
        proteomeDocument.proteomeType = 1;
        proteomeDocument.busco = 96.86911f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 0;
        return proteomeDocument;
    }

    private ProteomeDocument getDoc6() {
        ProteomeDocument proteomeDocument = new ProteomeDocument();
        proteomeDocument.upid = "UP000234681";
        proteomeDocument.organismName = new ArrayList<>(List.of("Rattus norvegicus", "Rat", "RAT"));
        proteomeDocument.organismSort = "Rattus norvegicus Rat RAT";
        proteomeDocument.organismTaxId = 10116;
        proteomeDocument.organismTaxon = new ArrayList<>(List.of("Rattus norvegicus", "Rat", "RAT", "scientificName for 10066",
                "commonName for 10066", "scientificName for 39107", "commonName for 39107", "scientificName for 10114", "commonName for 10114",
                "scientificName for 10116", "commonName for 10116"));
        proteomeDocument.taxLineageIds = new ArrayList<>(List.of(10116, 10066, 39107, 10114, 10116));
        proteomeDocument.strain = "BN; Sprague-Dawley";
        proteomeDocument.isRedundant = true;
        proteomeDocument.genomeAccession = new ArrayList<>(List.of("CM000234", "CM000247", "CM000245", "CM000239", "CM000240", "CM000250"
                , "CM000248", "CM000237", "CM000249", "CM000244", "CM000243", "CM000236", "CM000251", "CM000235", "CM000241", "CM000242", "CM000231"
                , "CM000246", "CM000232", "CM000238", "CM000233", "AAHX01000000"));
        proteomeDocument.genomeAssembly = new ArrayList<>(List.of("GCA_000002265.1"));
        proteomeDocument.score = 0;
        proteomeDocument.proteomeType = 3;
        proteomeDocument.busco = 81.65676f;
        proteomeDocument.cpd = 1;
        proteomeDocument.proteinCount = 0;
        return proteomeDocument;
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
