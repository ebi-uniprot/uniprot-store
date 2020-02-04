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
 * This class is responsible to load all the data for SuggestDocument and write it into HDFS
 *
 * @author lgonzales
 * @since 2020-01-15
 */
public class SuggestIndexer {

    /**
     * load all the data for SuggestDocument and write it into HDFS (Hadoop File System)
     *
     * @param sparkContext spark configuration
     * @param applicationConfig config
     */
    public static void writeIndexDocumentsToHDFS(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {

        JavaRDD<String> flatFileRDD =
                (JavaRDD<String>)
                        UniProtKBRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig);
        int suggestPartition =
                Integer.parseInt(applicationConfig.getString("suggest.partition.size"));
        JavaRDD<SuggestDocument> suggestRDD =
                getMain(sparkContext) // MAIN
                        .union(getKeyword(sparkContext, applicationConfig))
                        .union(getSubcell(sparkContext, applicationConfig))
                        .union(getEC(flatFileRDD, sparkContext, applicationConfig))
                        .union(getChebi(flatFileRDD, sparkContext, applicationConfig))
                        .union(getGo(flatFileRDD, sparkContext, applicationConfig))
                        .union(getOrganism(flatFileRDD, sparkContext, applicationConfig))
                        .repartition(suggestPartition);
        String hdfsPath = applicationConfig.getString("suggest.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(suggestRDD, hdfsPath);
    }

    /**
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument for uniprotkb main text search field
     */
    private static JavaRDD<SuggestDocument> getMain(JavaSparkContext sparkContext) {
        List<SuggestDocument> mainList = SuggestionConfig.databaseSuggestions();
        return (JavaRDD<SuggestDocument>) sparkContext.parallelize(mainList);
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with GOTerms (including ancestors) mapped from UniprotKB
     *     flat file entries
     */
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
                                .reduceByKey((goTermId1, goTermId2) -> goTermId1);

        return (JavaRDD<SuggestDocument>)
                goRelationsRDD
                        .join(goMapRDD)
                        .flatMapValues(new GOToSuggestDocument())
                        .values()
                        .distinct();
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with Chebi information mapped from UniprotKB flat file
     *     entries
     */
    private static JavaRDD<SuggestDocument> getChebi(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {

        // JavaPairRDD<chebiId,Chebi Entry> --> extracted from chebi.obo
        JavaPairRDD<String, Chebi> chebiRDD = ChebiRDDReader.load(sparkContext, applicationConfig);

        // JavaPairRDD<chebiId,chebiId> flatFileCatalyticActivityRDD --> extracted from flat file
        // CC(CatalyticActivity) lines
        JavaPairRDD<String, String> flatFileCatalyticActivityRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToCatalyticActivityChebi())
                                .reduceByKey((chebiId1, chebiId2) -> chebiId1);

        JavaRDD<SuggestDocument> catalyticActivitySuggest =
                (JavaRDD<SuggestDocument>)
                        flatFileCatalyticActivityRDD
                                .join(chebiRDD)
                                .mapValues(
                                        new ChebiToSuggestDocument(
                                                SuggestDictionary.CATALYTIC_ACTIVITY.name()))
                                .values()
                                .distinct();

        // JavaPairRDD<chebiId,chebiId> flatFileCofactorRDD --> extracted from flat file
        // CC(Cofactor) lines
        JavaPairRDD<String, String> flatFileCofactorRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToCofactorChebi())
                                .reduceByKey((chebiId1, chebiId2) -> chebiId1);

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

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with EC information mapped from UniprotKB flat file
     *     entries
     */
    private static JavaRDD<SuggestDocument> getEC(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {

        // JavaPairRDD<ecId,ecId> flatFileEcRDD --> extracted from flat file DE(with EC) lines
        JavaPairRDD<String, String> flatFileEcRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToEC())
                                .reduceByKey((ecId1, ecId2) -> ecId1);

        // JavaPairRDD<ecId,EC entry> ecRDD --> extracted from ec files
        JavaPairRDD<String, EC> ecRDD = ECRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                flatFileEcRDD.join(ecRDD).mapValues(new ECToSuggestDocument()).values().distinct();
    }

    /**
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with Subcellular Location information mapped from
     *     subcell.txt file
     */
    private static JavaRDD<SuggestDocument> getSubcell(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {

        // JavaPairRDD<subcellId,SubcellularLocationEntry> subcellularLocation --> extracted from
        // subcell.txt
        JavaPairRDD<String, SubcellularLocationEntry> subcellularLocation =
                SubcellularLocationRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                subcellularLocation
                        .mapValues(new SubcellularLocationToSuggestDocument())
                        .values()
                        .distinct();
    }

    /**
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with Keyword information mapped from keywlist.txt file
     */
    private static JavaRDD<SuggestDocument> getKeyword(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {

        // JavaPairRDD<keywordId,KeywordEntry> keyword --> extracted from keywlist.txt
        JavaPairRDD<String, KeywordEntry> keyword =
                KeywordRDDReader.load(sparkContext, applicationConfig);

        return (JavaRDD<SuggestDocument>)
                keyword.mapValues(new KeywordToSuggestDocument()).values().distinct();
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @param applicationConfig config
     * @return JavaRDD of SuggestDocument with Organism/Organism Host and Taxonomy information
     *     mapped from UniprotKB flat file entries
     */
    private static JavaRDD<SuggestDocument> getOrganism(
            JavaRDD<String> flatFileRDD,
            JavaSparkContext sparkContext,
            ResourceBundle applicationConfig) {

        JavaPairRDD<String, List<TaxonomyLineage>> organismWithLineage =
                TaxonomyLineageReader.load(sparkContext, applicationConfig, true);
        organismWithLineage.repartition(organismWithLineage.getNumPartitions());

        // ORGANISM
        // JavaPairRDD<taxId, taxId> flatFileOrganismRDD -> extract from flat file OX line
        JavaPairRDD<String, String> flatFileOrganismRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .mapToPair(new FlatFileToOrganism())
                                .reduceByKey((taxId1, taxId2) -> taxId1);

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

        // JavaPairRDD<taxId, taxId> flatFileOrganismHostRDD -> extract from flat file OH lines
        JavaPairRDD<String, String> flatFileOrganismHostRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToOrganismHost())
                                .reduceByKey((taxId1, taxId2) -> taxId1);
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

    /**
     * Load High Important Organism documents from a config file defined by Curators
     *
     * @param sparkContext spark config
     * @param dictionary Suggest Dictionary
     * @return JavaPairRDD<organismId, SuggestDocument>
     */
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
