package org.uniprot.store.spark.indexer.suggest;

import static org.uniprot.store.search.document.suggest.SuggestDictionary.*;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

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
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.chebi.ChebiRDDReader;
import org.uniprot.store.spark.indexer.common.JobParameter;
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

    private final JavaSparkContext sparkContext;
    private final ResourceBundle config;
    private final JobParameter jobParameter;

    public SuggestDocumentsToHDFSWriter(JobParameter jobParameter) {
        this.config = jobParameter.getApplicationConfig();
        this.sparkContext = jobParameter.getSparkContext();
        this.jobParameter = jobParameter;
    }
    /** load all the data for SuggestDocument and write it into HDFS (Hadoop File System) */
    @Override
    public void writeIndexDocumentsToHDFS() {
        UniProtKBRDDTupleReader flatFileReader = new UniProtKBRDDTupleReader(jobParameter, false);
        JavaRDD<String> flatFileRDD = flatFileReader.loadFlatFileToRDD();
        int suggestPartition = Integer.parseInt(config.getString("suggest.partition.size"));
        JavaRDD<SuggestDocument> suggestRDD =
                getMain()
                        .union(getKeyword())
                        .union(getSubcell())
                        .union(getEC(flatFileRDD))
                        .union(getChebi(flatFileRDD))
                        .union(getGo(flatFileRDD))
                        .union(getOrganism(flatFileRDD))
                        .repartition(suggestPartition);
        String hdfsPath =
                getCollectionOutputReleaseDirPath(
                        config, jobParameter.getReleaseName(), SolrCollection.suggest);
        SolrUtils.saveSolrInputDocumentRDD(suggestRDD, hdfsPath);
    }

    /** @return JavaRDD of SuggestDocument for uniprotkb main text search field */
    JavaRDD<SuggestDocument> getMain() {
        List<SuggestDocument> mainList = SuggestionConfig.databaseSuggestions();
        return sparkContext.parallelize(mainList);
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @return JavaRDD of SuggestDocument with GOTerms (including ancestors) mapped from UniprotKB
     *     flat file entries
     */
    JavaRDD<SuggestDocument> getGo(JavaRDD<String> flatFileRDD) {

        // JavaPairRDD<goId, GoTerm>
        GORelationRDDReader goReader = new GORelationRDDReader(jobParameter);
        JavaPairRDD<String, GeneOntologyEntry> goRelationsRDD = goReader.load();

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO
        JavaPairRDD<String, String> goMapRDD =
                flatFileRDD
                        .flatMapToPair(new GoRelationsJoinMapper())
                        .reduceByKey((goTermId1, goTermId2) -> goTermId1);

        return goRelationsRDD
                .join(goMapRDD)
                .flatMapValues(new GOToSuggestDocument())
                .values()
                .distinct();
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @return JavaRDD of SuggestDocument with ChebiEntry information mapped from UniprotKB flat
     *     file entries
     */
    JavaRDD<SuggestDocument> getChebi(JavaRDD<String> flatFileRDD) {

        // JavaPairRDD<chebiId,ChebiEntry Entry> --> extracted from chebi.obo
        ChebiRDDReader chebiReader = new ChebiRDDReader(jobParameter);
        JavaPairRDD<String, ChebiEntry> chebiRDD = chebiReader.load();

        // JavaPairRDD<chebiId,chebiId> flatFileCatalyticActivityRDD --> extracted from flat file
        // CC(CatalyticActivity) lines
        JavaPairRDD<String, String> flatFileCatalyticActivityRDD =
                flatFileRDD
                        .flatMapToPair(new FlatFileToCatalyticActivityChebi())
                        .reduceByKey((chebiId1, chebiId2) -> chebiId1);

        JavaRDD<SuggestDocument> catalyticActivitySuggest =
                flatFileCatalyticActivityRDD
                        .join(chebiRDD)
                        .mapValues(new ChebiToSuggestDocument(CATALYTIC_ACTIVITY.name()))
                        .values()
                        .distinct();

        // JavaPairRDD<chebiId,chebiId> flatFileCofactorRDD --> extracted from flat file
        // CC(Cofactor) lines
        JavaPairRDD<String, String> flatFileCofactorRDD =
                flatFileRDD
                        .flatMapToPair(new FlatFileToCofactorChebi())
                        .reduceByKey((chebiId1, chebiId2) -> chebiId1);

        JavaRDD<SuggestDocument> cofactorSuggest =
                flatFileCofactorRDD
                        .join(chebiRDD)
                        .mapValues(new ChebiToSuggestDocument(CHEBI.name()))
                        .values()
                        .distinct();

        return catalyticActivitySuggest.union(cofactorSuggest);
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @return JavaRDD of SuggestDocument with ECEntry information mapped from UniprotKB flat file
     *     entries
     */
    JavaRDD<SuggestDocument> getEC(JavaRDD<String> flatFileRDD) {

        // JavaPairRDD<ecId,ecId> flatFileEcRDD --> extracted from flat file DE(with ECEntry) lines
        JavaPairRDD<String, String> flatFileEcRDD =
                flatFileRDD.flatMapToPair(new FlatFileToEC()).reduceByKey((ecId1, ecId2) -> ecId1);

        // JavaPairRDD<ecId,ECEntry entry> ecRDD --> extracted from ec files
        ECRDDReader ecReader = new ECRDDReader(jobParameter);
        JavaPairRDD<String, ECEntry> ecRDD = ecReader.load();

        return flatFileEcRDD.join(ecRDD).mapValues(new ECToSuggestDocument()).values().distinct();
    }

    /**
     * @return JavaRDD of SuggestDocument with Subcellular Location information mapped from
     *     subcell.txt file
     */
    JavaRDD<SuggestDocument> getSubcell() {

        // JavaPairRDD<subcellId,SubcellularLocationEntry> subcellularLocation --> extracted from
        // subcell.txt
        SubcellularLocationRDDReader subReader = new SubcellularLocationRDDReader(jobParameter);
        JavaPairRDD<String, SubcellularLocationEntry> subcellularLocation = subReader.load();

        return subcellularLocation
                .mapValues(new SubcellularLocationToSuggestDocument())
                .values()
                .distinct();
    }

    /** @return JavaRDD of SuggestDocument with Keyword information mapped from keywlist.txt file */
    JavaRDD<SuggestDocument> getKeyword() {

        // JavaPairRDD<keywordId,KeywordEntry> keyword --> extracted from keywlist.txt
        KeywordRDDReader keywordReader = new KeywordRDDReader(jobParameter);
        JavaPairRDD<String, KeywordEntry> keyword = keywordReader.load();

        return keyword.mapValues(new KeywordToSuggestDocument()).values().distinct();
    }

    /**
     * @param flatFileRDD JavaRDD<flatFile entry in String format>
     * @return JavaRDD of SuggestDocument with Organism/Organism Host and Taxonomy information
     *     mapped from UniprotKB flat file entries
     */
    JavaRDD<SuggestDocument> getOrganism(JavaRDD<String> flatFileRDD) {

        TaxonomyLineageReader lineageReader = new TaxonomyLineageReader(jobParameter, true);
        JavaPairRDD<String, List<TaxonomyLineage>> organismWithLineage = lineageReader.load();
        organismWithLineage.repartition(organismWithLineage.getNumPartitions());

        // ORGANISM
        // JavaPairRDD<taxId, taxId> flatFileOrganismRDD -> extract from flat file OX line
        JavaPairRDD<String, String> flatFileOrganismRDD =
                flatFileRDD
                        .mapToPair(new FlatFileToOrganism())
                        .reduceByKey((taxId1, taxId2) -> taxId1);

        JavaRDD<SuggestDocument> organismSuggester =
                flatFileOrganismRDD
                        .join(organismWithLineage)
                        .mapValues(new OrganismToSuggestDocument(ORGANISM.name()))
                        .union(getDefaultHighImportantTaxon(ORGANISM))
                        .reduceByKey(new TaxonomyHighImportanceReduce())
                        .values();
        // TAXONOMY
        JavaRDD<SuggestDocument> taxonomySuggester =
                flatFileOrganismRDD
                        .join(organismWithLineage)
                        .flatMapToPair(new TaxonomyToSuggestDocument())
                        .union(getDefaultHighImportantTaxon(TAXONOMY))
                        .reduceByKey(new TaxonomyHighImportanceReduce())
                        .values();

        // JavaPairRDD<taxId, taxId> flatFileOrganismHostRDD -> extract from flat file OH lines
        JavaPairRDD<String, String> flatFileOrganismHostRDD =
                flatFileRDD
                        .flatMapToPair(new FlatFileToOrganismHost())
                        .reduceByKey((taxId1, taxId2) -> taxId1);
        // ORGANISM HOST
        JavaRDD<SuggestDocument> organismHostSuggester =
                flatFileOrganismHostRDD
                        .join(organismWithLineage)
                        .mapValues(new OrganismToSuggestDocument(HOST.name()))
                        .union(getDefaultHighImportantTaxon(HOST))
                        .reduceByKey(new TaxonomyHighImportanceReduce())
                        .values();

        return organismSuggester.union(taxonomySuggester).union(organismHostSuggester);
    }

    /**
     * Load High Important Organism documents from a config file defined by Curators
     *
     * @param dictionary Suggest Dictionary
     * @return JavaPairRDD<organismId, SuggestDocument>
     */
    JavaPairRDD<String, SuggestDocument> getDefaultHighImportantTaxon(
            SuggestDictionary dictionary) {
        SuggestionConfig suggestionConfig = new SuggestionConfig();
        List<SuggestDocument> suggestList = new ArrayList<>();
        if (dictionary.equals(TAXONOMY) || dictionary.equals(ORGANISM)) {
            suggestList.addAll(
                    suggestionConfig.loadDefaultTaxonSynonymSuggestions(
                            dictionary, SuggestionConfig.DEFAULT_TAXON_SYNONYMS_FILE));
        } else if (dictionary.equals(HOST)) {
            suggestList.addAll(
                    suggestionConfig.loadDefaultTaxonSynonymSuggestions(
                            dictionary, SuggestionConfig.DEFAULT_HOST_SYNONYMS_FILE));
        }
        List<Tuple2<String, SuggestDocument>> tupleList =
                suggestList.stream()
                        .map(suggest -> new Tuple2<>(suggest.id, suggest))
                        .collect(Collectors.toList());
        return sparkContext.parallelizePairs(tupleList);
    }
}
