package org.uniprot.store.spark.indexer.uniprot;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.cv.pathway.UniPathwayFileReader;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GORelationRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GOTerm;
import org.uniprot.store.spark.indexer.literature.LiteratureMappedRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.*;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniRefJoinMapper;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniRefMappedToUniprotDocument;
import org.uniprot.store.spark.indexer.uniprot.mapper.model.MappedUniRef;
import org.uniprot.store.spark.indexer.uniref.UniRefRDDTupleReader;
import org.uniprot.store.spark.indexer.util.SolrUtils;
import org.uniprot.store.spark.indexer.util.SparkUtils;

/**
 * This class is responsible to load all the data for UniProtDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class UniProtKBIndexer {

    public static void writeIndexDocumentsToHDFS(
            JavaSparkContext sparkContext, ResourceBundle applicationConfig) {
        SparkConf sparkConf = sparkContext.sc().conf();

        JavaPairRDD<String, UniProtEntry> uniProtEntryRDD =
                UniProtKBRDDTupleReader.load(sparkContext, applicationConfig);

        uniProtEntryRDD = joinGoEvidences(uniProtEntryRDD, applicationConfig, sparkConf);

        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD =
                convertToUniProtDocument(uniProtEntryRDD, applicationConfig, sparkContext);

        uniProtDocumentRDD = joinGoRelations(uniProtDocumentRDD, applicationConfig, sparkContext);

        uniProtDocumentRDD = joinTaxonomy(uniProtDocumentRDD, applicationConfig, sparkContext);

        uniProtDocumentRDD = joinAllUniRefs(uniProtDocumentRDD, applicationConfig, sparkConf);

        uniProtDocumentRDD = joinLiteratureMapped(uniProtDocumentRDD, applicationConfig, sparkConf);

        boolean shouldIndexInactive =
                Boolean.valueOf(applicationConfig.getString("uniprot.index.inactive"));
        if (shouldIndexInactive) {
            JavaPairRDD<String, UniProtDocument> inactiveEntryRDD =
                    InactiveUniProtKBRDDTupleReader.load(sparkConf, applicationConfig);
            uniProtDocumentRDD = uniProtDocumentRDD.union(inactiveEntryRDD);
        }

        String hdfsPath = applicationConfig.getString("uniprot.solr.documents.path");
        SolrUtils.saveSolrInputDocumentRDD(uniProtDocumentRDD, hdfsPath);

        log.info("Completed UniProtKB prepare Solr index");
    }

    /**
     * @param uniProtEntryRDD JavaPairRDD<accesion, UniProtEntry>
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with UniProtEntry mapped information
     */
    private static JavaPairRDD<String, UniProtDocument> convertToUniProtDocument(
            JavaPairRDD<String, UniProtEntry> uniProtEntryRDD,
            ResourceBundle applicationConfig,
            JavaSparkContext sparkContext) {

        Configuration hadoopConfig = sparkContext.hadoopConfiguration();
        Map<String, String> pathway = loadPathway(applicationConfig, hadoopConfig);
        return (JavaPairRDD<String, UniProtDocument>)
                uniProtEntryRDD.mapValues(new UniProtEntryToSolrDocument(pathway));
    }

    private static Map<String, String> loadPathway(
            ResourceBundle applicationConfig, Configuration hadoopConfig) {

        String filePath = applicationConfig.getString("pathway.file.path");
        UniPathwayFileReader uniPathwayFileReader = new UniPathwayFileReader();
        List<String> lines = SparkUtils.readLines(filePath, hadoopConfig);
        List<UniPathway> pathwayList = uniPathwayFileReader.parseLines(lines);
        return pathwayList.stream()
                .collect(Collectors.toMap(UniPathway::getName, UniPathway::getAccession));
    }

    /**
     * --- PLEASE NOTE --- To join Taxonomy we are creating an RDD of JavaPairRDD<taxId,accession>
     * extracted from FlatFile OX and OH lines. For example, the protein P26747 will have 2 tuples
     * in the RDD: Tuple2<10754,P26747> for organism Tuple2<90371,P26747> for virus host
     *
     * <p>The Second step is a join between JavaPairRDD<taxId,accession> and
     * JavaPairRDD<taxId,TaxonomyEntry> received as method parameter and we group by accession, so
     * the result RDD would be a (JavaPairRDD<accession, Iterable<TaxonomyEntry>>). For example, the
     * protein P26747 would have one tuple in the RDD: Tuple2<P26747, Iterable<TaxonomyEntry(10754),
     * TaxonomyEntry(90371)>>
     *
     * <p>The Third and last step is to join JavaPairRDD<accession, Iterable<TaxonomyEntry>> with
     * JavaPairRDD<accession, UniProtDocument> and at this point we can map TaxonomyEntry
     * information into UniProtDocument.
     *
     * @param uniProtDocumentRDD RDD of JavaPairRDD<accession,UniProtDocument>
     * @param applicationConfig config
     * @param sparkContext context
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with the mapped taxonomy information
     */
    private static JavaPairRDD<String, UniProtDocument> joinTaxonomy(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            ResourceBundle applicationConfig,
            JavaSparkContext sparkContext) {

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                TaxonomyRDDReader.loadWithLineage(sparkContext, applicationConfig);

        // JavaPairRDD<taxId,accession> taxonomyMapRDD --> extracted from flat file OX and OH lines
        JavaPairRDD<String, String> taxonomyMapRDD =
                (JavaPairRDD<String, String>)
                        UniProtKBRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig)
                                .flatMapToPair(new TaxonomyJoinMapper());

        // JavaPairRDD<accession, Iterable<taxonomy>> joinRDD
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD =
                (JavaPairRDD<String, Iterable<TaxonomyEntry>>)
                        JavaPairRDD.fromJavaRDD(
                                        taxonomyMapRDD.join(taxonomyEntryJavaPairRDD).values())
                                .groupByKey();

        return (JavaPairRDD<String, UniProtDocument>)
                uniProtDocumentRDD
                        .leftOuterJoin(joinedRDD)
                        .mapValues(new TaxonomyEntryToUniProtDocument());
    }

    /**
     * --- PLEASE NOTE --- To join GoRelations we are creating an RDD of JavaPairRDD<goId,accession>
     * extracted from FlatFile GO DR lines. It means that one protein may have many goIds
     *
     * <p>The Second step is a join between JavaPairRDD<goId,accession> and JavaPairRDD<goId,GoTerm>
     * received as method parameter and we group by accession, so the result RDD would be a
     * (JavaPairRDD<accession, Iterable<GoTerm>>).
     *
     * <p>The Third and last step is to join JavaPairRDD<accession, Iterable<GoTerm>> with
     * JavaPairRDD<accession, UniProtDocument> and at this point we can map GoTerms Relations
     * information into UniProtDocument.
     *
     * @param uniProtDocumentRDD JavaPairRDD<accession, UniProtDocument>
     * @param applicationConfig config
     * @param sparkContext spark context
     * @return JavaPairRDD<accession, UniProtDocument> with added GoRelations
     */
    private static JavaPairRDD<String, UniProtDocument> joinGoRelations(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            ResourceBundle applicationConfig,
            JavaSparkContext sparkContext) {

        // JavaPairRDD<goId, GoTerm>
        JavaPairRDD<String, GOTerm> goRelationsRDD =
                GORelationRDDReader.load(applicationConfig, sparkContext);

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO
        JavaPairRDD<String, String> goMapRDD =
                (JavaPairRDD<String, String>)
                        UniProtKBRDDTupleReader.loadFlatFileToRDD(sparkContext, applicationConfig)
                                .flatMapToPair(new GoRelationsJoinMapper());

        // JavaPairRDD<accession, Iterable<GoTerm>> joinRDD
        JavaPairRDD<String, Iterable<GOTerm>> joinedRDD =
                (JavaPairRDD<String, Iterable<GOTerm>>)
                        JavaPairRDD.fromJavaRDD(goMapRDD.join(goRelationsRDD).values())
                                .groupByKey();

        return (JavaPairRDD<String, UniProtDocument>)
                uniProtDocumentRDD
                        .leftOuterJoin(joinedRDD)
                        .mapValues(new GoRelationsToUniProtDocument());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @param applicationConfig config
     * @param sparkConf spark configuration
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with mapped UniRef information
     */
    private static JavaPairRDD<String, UniProtDocument> joinAllUniRefs(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            ResourceBundle applicationConfig,
            SparkConf sparkConf) {
        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD =
                loadUniRefMap(UniRefType.UniRef50, sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref50EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD =
                loadUniRefMap(UniRefType.UniRef90, sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref90EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD =
                loadUniRefMap(UniRefType.UniRef100, sparkConf, applicationConfig);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref100EntryRDD);

        return uniProtDocumentRDD;
    }

    /** @return JavaPairRDD{Key=accession, value=MappedUniRef} for UniRefType.UniRef90 */
    private static JavaPairRDD<String, MappedUniRef> loadUniRefMap(
            UniRefType uniRefType, SparkConf sparkConf, ResourceBundle applicationConfig) {
        JavaRDD<UniRefEntry> uniRefEntryJavaRDD =
                UniRefRDDTupleReader.load(uniRefType, sparkConf, applicationConfig);
        return (JavaPairRDD<String, MappedUniRef>)
                uniRefEntryJavaRDD.flatMapToPair(new UniRefJoinMapper());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @param unirefJavaPair JavaPairRDD<accesion, MappedUniRef>
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with MappedUniRef mapped information
     */
    private static JavaPairRDD<String, UniProtDocument> joinUniRef(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, MappedUniRef> unirefJavaPair) {

        return (JavaPairRDD<String, UniProtDocument>)
                uniProtDocumentRDD
                        .leftOuterJoin(unirefJavaPair)
                        .mapValues(new UniRefMappedToUniprotDocument());
    }

    /**
     * @param uniProtEntryRDD JavaPairRDD<accesion, UniProtEntry>
     * @param applicationConfig config
     * @param sparkConf spark configuration
     * @return RDD of JavaPairRDD<accesion, UniProtEntry> with extended GoEvidence mapped
     *     information
     */
    private static JavaPairRDD<String, UniProtEntry> joinGoEvidences(
            JavaPairRDD<String, UniProtEntry> uniProtEntryRDD,
            ResourceBundle applicationConfig,
            SparkConf sparkConf) {
        JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD =
                GOEvidencesRDDReader.load(sparkConf, applicationConfig);
        return (JavaPairRDD<String, UniProtEntry>)
                uniProtEntryRDD.leftOuterJoin(goEvidenceRDD).mapValues(new GOEvidenceMapper());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @param applicationConfig config
     * @param sparkConf spark configuration
     * @return JavaPairRDD<accesion, UniProtDocument> with mapped PIR Computationally mapped pubmed
     *     ids
     */
    private static JavaPairRDD<String, UniProtDocument> joinLiteratureMapped(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            ResourceBundle applicationConfig,
            SparkConf sparkConf) {
        JavaPairRDD<String, Iterable<String>> literatureMappedRDD =
                LiteratureMappedRDDReader.loadAccessionPubMedRDD(sparkConf, applicationConfig);
        return (JavaPairRDD<String, UniProtDocument>)
                uniProtDocumentRDD
                        .leftOuterJoin(literatureMappedRDD)
                        .mapValues(new LiteratureMappedToUniProtDocument());
    }
}
