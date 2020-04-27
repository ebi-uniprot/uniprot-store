package org.uniprot.store.spark.indexer.suggest;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getOutputReleaseDirPath;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.chebi.ChebiRDDReader;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.ec.ECRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GORelationRDDReader;
import org.uniprot.store.spark.indexer.keyword.KeywordRDDReader;
import org.uniprot.store.spark.indexer.subcell.SubcellularLocationRDDReader;
import org.uniprot.store.spark.indexer.suggest.mapper.TaxonomyHighImportanceReduce;
import org.uniprot.store.spark.indexer.suggest.mapper.document.*;
import org.uniprot.store.spark.indexer.suggest.mapper.flatfile.*;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyLineageReader;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.GoRelationsJoinMapper;

import scala.Tuple2;

/**
 * This class is responsible to load all the data for SuggestDocument and write it into HDFS
 *
 * @author lgonzales
 * @since 2020-01-15
 */
public class SuggestDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final ResourceBundle applicationConfig;

    public SuggestDocumentsToHDFSWriter(ResourceBundle applicationConfig) {
        this.applicationConfig = applicationConfig;
    }

    /**
     * load all the data for SuggestDocument and write it into HDFS (Hadoop File System)
     *
     * @param sparkContext spark configuration
     * @param releaseName release name
     */
    @Override
    public void writeIndexDocumentsToHDFS(JavaSparkContext sparkContext, String releaseName) {
        JavaRDD<String> flatFileRDD =
                (JavaRDD<String>)
                        UniProtKBRDDTupleReader.loadFlatFileToRDD(
                                sparkContext, applicationConfig, releaseName);
        int suggestPartition =
                Integer.parseInt(applicationConfig.getString("suggest.partition.size"));
        JavaRDD<SuggestDocument> suggestRDD =
                getMain(sparkContext) // MAIN
                        .union(getKeyword(sparkContext, releaseName))
                        .union(getSubcell(sparkContext, releaseName))
                        .union(getEC(flatFileRDD, sparkContext, releaseName))
                        .union(getChebi(flatFileRDD, sparkContext, releaseName))
                        .union(getGo(flatFileRDD, sparkContext, releaseName))
                        .union(getOrganism(flatFileRDD, sparkContext))
                        .repartition(suggestPartition);
        String releaseOutputDir = getOutputReleaseDirPath(applicationConfig, releaseName);
        String hdfsPath =
                releaseOutputDir + applicationConfig.getString("suggest.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(suggestRDD, hdfsPath);
    }

    /**
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument for uniprotkb main text search field
     */
    private JavaRDD<SuggestDocument> getMain(JavaSparkContext sparkContext) {
        List<SuggestDocument> mainList = SuggestionConfig.databaseSuggestions();
        return (JavaRDD<SuggestDocument>) sparkContext.parallelize(mainList);
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument with GOTerms (including ancestors) mapped from UniprotKB
     *     flat file entries
     */
    private JavaRDD<SuggestDocument> getGo(
            JavaRDD<String> flatFileRDD, JavaSparkContext sparkContext, String releaseName) {

        // JavaPairRDD<goId, GoTerm>
        JavaPairRDD<String, GeneOntologyEntry> goRelationsRDD =
                GORelationRDDReader.load(applicationConfig, sparkContext, releaseName);

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
     * @return JavaRDD of SuggestDocument with ChebiEntry information mapped from UniprotKB flat
     *     file entries
     */
    private JavaRDD<SuggestDocument> getChebi(
            JavaRDD<String> flatFileRDD, JavaSparkContext sparkContext, String releaseName) {

        // JavaPairRDD<chebiId,ChebiEntry Entry> --> extracted from chebi.obo
        JavaPairRDD<String, ChebiEntry> chebiRDD =
                ChebiRDDReader.load(sparkContext, applicationConfig, releaseName);

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
     * @return JavaRDD of SuggestDocument with ECEntry information mapped from UniprotKB flat file
     *     entries
     */
    private JavaRDD<SuggestDocument> getEC(
            JavaRDD<String> flatFileRDD, JavaSparkContext sparkContext, String releaseName) {

        // JavaPairRDD<ecId,ecId> flatFileEcRDD --> extracted from flat file DE(with ECEntry) lines
        JavaPairRDD<String, String> flatFileEcRDD =
                (JavaPairRDD<String, String>)
                        flatFileRDD
                                .flatMapToPair(new FlatFileToEC())
                                .reduceByKey((ecId1, ecId2) -> ecId1);

        // JavaPairRDD<ecId,ECEntry entry> ecRDD --> extracted from ec files
        JavaPairRDD<String, ECEntry> ecRDD =
                ECRDDReader.load(sparkContext, applicationConfig, releaseName);

        return (JavaRDD<SuggestDocument>)
                flatFileEcRDD.join(ecRDD).mapValues(new ECToSuggestDocument()).values().distinct();
    }

    /**
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument with Subcellular Location information mapped from
     *     subcell.txt file
     */
    private JavaRDD<SuggestDocument> getSubcell(JavaSparkContext sparkContext, String releaseName) {

        // JavaPairRDD<subcellId,SubcellularLocationEntry> subcellularLocation --> extracted from
        // subcell.txt
        JavaPairRDD<String, SubcellularLocationEntry> subcellularLocation =
                SubcellularLocationRDDReader.load(sparkContext, applicationConfig, releaseName);

        return (JavaRDD<SuggestDocument>)
                subcellularLocation
                        .mapValues(new SubcellularLocationToSuggestDocument())
                        .values()
                        .distinct();
    }

    /**
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument with Keyword information mapped from keywlist.txt file
     */
    private JavaRDD<SuggestDocument> getKeyword(JavaSparkContext sparkContext, String releaseName) {

        // JavaPairRDD<keywordId,KeywordEntry> keyword --> extracted from keywlist.txt
        JavaPairRDD<String, KeywordEntry> keyword =
                KeywordRDDReader.load(sparkContext, applicationConfig, releaseName);

        return (JavaRDD<SuggestDocument>)
                keyword.mapValues(new KeywordToSuggestDocument()).values().distinct();
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @param sparkContext spark configuration
     * @return JavaRDD of SuggestDocument with Organism/Organism Host and Taxonomy information
     *     mapped from UniprotKB flat file entries
     */
    private JavaRDD<SuggestDocument> getOrganism(
            JavaRDD<String> flatFileRDD, JavaSparkContext sparkContext) {

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
    private JavaPairRDD<String, SuggestDocument> getDefaultHighImportantTaxon(
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
                        .map(suggest -> new Tuple2<>(suggest.id, suggest))
                        .collect(Collectors.toList());
        return (JavaPairRDD<String, SuggestDocument>) sparkContext.parallelizePairs(tupleList);
    }
}
