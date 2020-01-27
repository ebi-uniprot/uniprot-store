package org.uniprot.store.spark.indexer.suggest;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.chebi.Chebi;
import org.uniprot.core.cv.ec.EC;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.chebi.ChebiRDDReader;
import org.uniprot.store.spark.indexer.ec.ECRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GORelationRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GOTerm;
import org.uniprot.store.spark.indexer.keyword.KeywordRDDReader;
import org.uniprot.store.spark.indexer.subcell.SubcellularLocationRDDReader;
import org.uniprot.store.spark.indexer.suggest.mapper.TaxonomyHighImportanceReduce;
import org.uniprot.store.spark.indexer.suggest.mapper.document.*;
import org.uniprot.store.spark.indexer.suggest.mapper.flatfile.*;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyLineageReader;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoRelationsJoinMapper;
import org.uniprot.store.spark.indexer.util.SolrUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-15
 */
public class SuggestIndexer {

    public static void writeIndexDocumentsToHDFS(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {

        JavaRDD<String> flatFileRDD =
                (JavaRDD<String>)
                        UniProtKBRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig);
        int suggestPartition =
                Integer.parseInt(applicationConfig.getString("suggest.partition.size"));
        JavaRDD<SuggestDocument> suggestRDD =
                getMain(sparkContext) // MAIN
                        .union(getKeyword(flatFileRDD, sparkContext, applicationConfig)) // KEYWORD
                        .union(getSubcell(flatFileRDD, sparkContext, applicationConfig)) // SUBCELL
                        .union(getEC(flatFileRDD, sparkContext, applicationConfig)) // EC
                        .union(
                                getChebi(
                                        flatFileRDD,
                                        sparkContext,
                                        applicationConfig)) // CHEBI + CATALYTIC_ACTIVITY
                        .union(getGo(flatFileRDD, sparkContext, applicationConfig)) // GO
                        .union(
                                getOrganism(
                                        flatFileRDD,
                                        sparkContext,
                                        applicationConfig)) // ORGANISM, TAXONOMY, HOST
                        .repartition(suggestPartition);
        /*        suggestRDD.foreach(
        doc -> {
            System.out.println(doc.dictionary + ":" + doc.id + ":" + doc.value);
        });*/
        String hdfsPath = applicationConfig.getString("suggest.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(suggestRDD, hdfsPath);
    }

    private static JavaRDD<SuggestDocument> getMain(JavaSparkContext sparkContext) {
        List<SuggestDocument> mainList = SuggestionConfig.databaseSuggestions();
        return (JavaRDD<SuggestDocument>) sparkContext.parallelize(mainList);
    }

    private static JavaRDD<SuggestDocument> getGo(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {

        // JavaPairRDD<goId, GoTerm>
        JavaPairRDD<String, GOTerm> goRelationsRDD =
                GORelationRDDReader.load(applicationConfig, sparkContext);

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO
        JavaPairRDD<String, String> goMapRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new GoRelationsJoinMapper())
                                .reduceByKey((v1, v2) -> v1);

        return (JavaRDD<SuggestDocument>)
                goRelationsRDD
                        .join(goMapRDD)
                        .flatMapValues(new GOToSuggestDocument())
                        .values()
                        .distinct();
    }

    private static JavaRDD<SuggestDocument> getChebi(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {
        JavaPairRDD<String, Chebi> chebiRDD = ChebiRDDReader.load(sparkContext, applicationConfig);

        JavaPairRDD<String, String> flatFileCatalyticActivityRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToCatalyticActivityChebi())
                                .reduceByKey((v1, v2) -> v1);

        JavaRDD<SuggestDocument> catalyticActivitySuggest =
                (JavaRDD<SuggestDocument>)
                        flatFileCatalyticActivityRDD
                                .join(chebiRDD)
                                .mapValues(
                                        new ChebiToSuggestDocument(
                                                SuggestDictionary.CATALYTIC_ACTIVITY.name()))
                                .values()
                                .distinct();

        JavaPairRDD<String, String> flatFileCofactorRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToCofactorChebi())
                                .reduceByKey((v1, v2) -> v1);

        JavaRDD<SuggestDocument> cofactorSuggest =
                (JavaRDD<SuggestDocument>)
                        flatFileCofactorRDD
                                .join(chebiRDD)
                                .mapValues(
                                        new ChebiToSuggestDocument(SuggestDictionary.CHEBI.name()))
                                .values()
                                .distinct();

        return catalyticActivitySuggest.union(cofactorSuggest);
    }

    private static JavaRDD<SuggestDocument> getEC(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {
        JavaPairRDD<String, String> flatFileEcRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD.flatMapToPair(new FlatFileToEC()).reduceByKey((v1, v2) -> v1);

        JavaPairRDD<String, EC> ecRDD = ECRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                flatFileEcRDD.join(ecRDD).mapValues(new ECToSuggestDocument()).values().distinct();
    }

    private static JavaRDD<SuggestDocument> getSubcell(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {
        JavaPairRDD<String, String> flatFileSubcellRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToSubcellularLocation())
                                .reduceByKey((v1, v2) -> v1);

        JavaPairRDD<String, SubcellularLocationEntry> subcellularLocation =
                SubcellularLocationRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                flatFileSubcellRDD
                        .join(subcellularLocation)
                        .mapValues(new SubcellularLocationToSuggestDocument())
                        .values()
                        .distinct();
    }

    private static JavaRDD<SuggestDocument> getKeyword(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {
        JavaPairRDD<String, String> flatFileKeywordRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToKeyword())
                                .reduceByKey((v1, v2) -> v1);

        JavaPairRDD<String, KeywordEntry> keyword =
                KeywordRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                flatFileKeywordRDD
                        .join(keyword)
                        .flatMapValues(new KeywordToSuggestDocument())
                        .values()
                        .distinct();
    }

    private static JavaRDD<SuggestDocument> getOrganism(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {

        JavaPairRDD<String, List<TaxonomyLineage>> organismWithLineage =
                TaxonomyLineageReader.load(sparkContext, applicationConfig, true);
        organismWithLineage.repartition(organismWithLineage.getNumPartitions());

        // ORGANISM
        JavaPairRDD<String, String> flatFileOrganismRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD.mapToPair(new FlatFileToOrganism()).reduceByKey((v1, v2) -> v1);

        JavaRDD<SuggestDocument> organismSuggester =
                (JavaRDD<SuggestDocument>)
                        flatFileOrganismRDD
                                .join(organismWithLineage)
                                .mapValues(
                                        new OrganismToSuggestDocument(
                                                SuggestDictionary.ORGANISM.name()))
                                .union(
                                        getDefaultHighImportantTaxon(
                                                sparkContext, SuggestDictionary.ORGANISM))
                                .reduceByKey(new TaxonomyHighImportanceReduce())
                                .values();
        // TAXONOMY
        JavaRDD<SuggestDocument> taxonomySuggester =
                (JavaRDD<SuggestDocument>)
                        flatFileOrganismRDD
                                .join(organismWithLineage)
                                .flatMapToPair(new TaxonomyToSuggestDocument())
                                .union(
                                        getDefaultHighImportantTaxon(
                                                sparkContext, SuggestDictionary.TAXONOMY))
                                .reduceByKey(new TaxonomyHighImportanceReduce())
                                .values();

        JavaPairRDD<String, String> flatFileOrganismHostRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToOrganismHost())
                                .reduceByKey((v1, v2) -> v1);
        // ORGANISM HOST
        JavaRDD<SuggestDocument> organismHostSuggester =
                (JavaRDD<SuggestDocument>)
                        flatFileOrganismHostRDD
                                .join(organismWithLineage)
                                .mapValues(
                                        new OrganismToSuggestDocument(
                                                SuggestDictionary.HOST.name()))
                                .union(
                                        getDefaultHighImportantTaxon(
                                                sparkContext, SuggestDictionary.HOST))
                                .reduceByKey(new TaxonomyHighImportanceReduce())
                                .values();

        return organismSuggester.union(taxonomySuggester).union(organismHostSuggester);
    }

    private static JavaPairRDD<String, SuggestDocument> getDefaultHighImportantTaxon(
            JavaSparkContext sparkContext, SuggestDictionary dictionary) {
        SuggestionConfig config = new SuggestionConfig();
        List<SuggestDocument> suggestList = new ArrayList<>();
        if (dictionary.equals(SuggestDictionary.TAXONOMY)
                || dictionary.equals(SuggestDictionary.ORGANISM)) {
            suggestList.addAll(
                    config.loadDefaultTaxonSynonymSuggestions(
                            dictionary, SuggestionConfig.DEFAULT_TAXON_SYNONYMS_FILE));
        } else if (dictionary.equals(SuggestDictionary.HOST)) {
            suggestList.addAll(
                    config.loadDefaultTaxonSynonymSuggestions(
                            dictionary, SuggestionConfig.DEFAULT_HOST_SYNONYMS_FILE));
        }
        List<Tuple2<String, SuggestDocument>> tupleList =
                suggestList.stream()
                        .map(
                                suggest -> {
                                    return new Tuple2<>(suggest.id, suggest);
                                })
                        .collect(Collectors.toList());
        return (JavaPairRDD<String, SuggestDocument>) sparkContext.parallelizePairs(tupleList);
    }
}
