package org.uniprot.store.spark.indexer.uniprot;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.cv.pathway.UniPathwayFileReader;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniprot.UniProtDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidence;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidenceMapper;
import org.uniprot.store.spark.indexer.go.evidence.GOEvidencesRDDReader;
import org.uniprot.store.spark.indexer.go.relations.GORelationRDDReader;
import org.uniprot.store.spark.indexer.literature.LiteratureMappedRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniprot.mapper.*;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniRefJoinMapper;
import org.uniprot.store.spark.indexer.uniprot.mapper.UniRefMappedToUniprotDocument;
import org.uniprot.store.spark.indexer.uniprot.mapper.model.MappedUniRef;
import org.uniprot.store.spark.indexer.uniref.UniRefRDDTupleReader;

/**
 * This class is responsible to load all the data for UniProtDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2019-11-12
 */
@Slf4j
public class UniProtKBDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JavaSparkContext sparkContext;
    private final String releaseName;
    private final ResourceBundle config;
    private final JobParameter parameter;

    public UniProtKBDocumentsToHDFSWriter(JobParameter jobParameter) {
        this.config = jobParameter.getApplicationConfig();
        this.releaseName = jobParameter.getReleaseName();
        this.sparkContext = jobParameter.getSparkContext();
        this.parameter = jobParameter;
    }
    /** load all the data for UniProtDocument and write it into HDFS (Hadoop File System) */
    @Override
    public void writeIndexDocumentsToHDFS() {
        JavaPairRDD<String, UniProtKBEntry> uniProtEntryRDD =
                UniProtKBRDDTupleReader.load(parameter, true);

        uniProtEntryRDD = joinGoEvidences(uniProtEntryRDD);

        JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD =
                convertToUniProtDocument(uniProtEntryRDD);

        uniProtDocumentRDD = joinGoRelations(uniProtDocumentRDD);

        uniProtDocumentRDD = joinTaxonomy(uniProtDocumentRDD);

        uniProtDocumentRDD = joinAllUniRefs(uniProtDocumentRDD);

        uniProtDocumentRDD = joinLiteratureMapped(uniProtDocumentRDD);

        boolean shouldIndexInactive =
                Boolean.parseBoolean(config.getString("uniprot.index.inactive"));
        if (shouldIndexInactive) {
            JavaPairRDD<String, UniProtDocument> inactiveEntryRDD =
                    InactiveUniProtKBRDDTupleReader.load(parameter);
            uniProtDocumentRDD = uniProtDocumentRDD.union(inactiveEntryRDD);
        }

        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.uniprot);
        SolrUtils.saveSolrInputDocumentRDD(uniProtDocumentRDD, hdfsPath);

        log.info("Completed UniProtKB prepare Solr index");
    }

    /**
     * @param uniProtEntryRDD JavaPairRDD<accesion, UniProtKBEntry>
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with UniProtKBEntry mapped information
     */
    private JavaPairRDD<String, UniProtDocument> convertToUniProtDocument(
            JavaPairRDD<String, UniProtKBEntry> uniProtEntryRDD) {

        Configuration hadoopConfig = sparkContext.hadoopConfiguration();
        Map<String, String> pathway = loadPathway(hadoopConfig, releaseName);
        return uniProtEntryRDD.mapValues(new UniProtEntryToSolrDocument(pathway));
    }

    private Map<String, String> loadPathway(Configuration hadoopConfig, String releaseName) {
        String releaseInputDir = getInputReleaseMainThreadDirPath(config, releaseName);
        String filePath = releaseInputDir + config.getString("pathway.file.path");
        UniPathwayFileReader uniPathwayFileReader = new UniPathwayFileReader();
        List<String> lines = SparkUtils.readLines(filePath, hadoopConfig);
        List<UniPathway> pathwayList = uniPathwayFileReader.parseLines(lines);
        return pathwayList.stream()
                .collect(Collectors.toMap(UniPathway::getName, UniPathway::getId));
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
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with the mapped taxonomy information
     */
    private JavaPairRDD<String, UniProtDocument> joinTaxonomy(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD) {

        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                TaxonomyRDDReader.loadWithLineage(sparkContext, config);

        // JavaPairRDD<taxId,accession> taxonomyMapRDD --> extracted from flat file OX and OH lines
        JavaPairRDD<String, String> taxonomyMapRDD =
                UniProtKBRDDTupleReader.loadFlatFileToRDD(parameter)
                        .flatMapToPair(new TaxonomyJoinMapper());

        // JavaPairRDD<accession, Iterable<taxonomy>> joinRDD
        JavaPairRDD<String, Iterable<TaxonomyEntry>> joinedRDD =
                JavaPairRDD.fromJavaRDD(taxonomyMapRDD.join(taxonomyEntryJavaPairRDD).values())
                        .groupByKey();

        return uniProtDocumentRDD
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
     * @return JavaPairRDD<accession, UniProtDocument> with added GoRelations
     */
    private JavaPairRDD<String, UniProtDocument> joinGoRelations(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD) {

        // JavaPairRDD<goId, GoTerm>
        JavaPairRDD<String, GeneOntologyEntry> goRelationsRDD = GORelationRDDReader.load(parameter);

        // JavaPairRDD<goId,accession> goMapRDD --> extracted from flat file DR lines for GO
        JavaPairRDD<String, String> goMapRDD =
                UniProtKBRDDTupleReader.loadFlatFileToRDD(parameter)
                        .flatMapToPair(new GoRelationsJoinMapper());

        // JavaPairRDD<accession, Iterable<GoTerm>> joinRDD
        JavaPairRDD<String, Iterable<GeneOntologyEntry>> joinedRDD =
                JavaPairRDD.fromJavaRDD(goMapRDD.join(goRelationsRDD).values()).groupByKey();

        return uniProtDocumentRDD
                .leftOuterJoin(joinedRDD)
                .mapValues(new GoRelationsToUniProtDocument());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with mapped UniRef information
     */
    private JavaPairRDD<String, UniProtDocument> joinAllUniRefs(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD) {
        JavaPairRDD<String, MappedUniRef> uniref50EntryRDD = loadUniRefMap(UniRefType.UniRef50);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref50EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref90EntryRDD = loadUniRefMap(UniRefType.UniRef90);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref90EntryRDD);

        JavaPairRDD<String, MappedUniRef> uniref100EntryRDD = loadUniRefMap(UniRefType.UniRef100);
        uniProtDocumentRDD = joinUniRef(uniProtDocumentRDD, uniref100EntryRDD);

        return uniProtDocumentRDD;
    }

    /** @return JavaPairRDD{Key=accession, value=MappedUniRef} for UniRefType.UniRef90 */
    private JavaPairRDD<String, MappedUniRef> loadUniRefMap(UniRefType uniRefType) {
        JavaRDD<UniRefEntry> uniRefEntryJavaRDD =
                UniRefRDDTupleReader.load(uniRefType, parameter, true);
        return uniRefEntryJavaRDD.flatMapToPair(new UniRefJoinMapper());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @param unirefJavaPair JavaPairRDD<accesion, MappedUniRef>
     * @return RDD of JavaPairRDD<accesion, UniProtDocument> with MappedUniRef mapped information
     */
    private JavaPairRDD<String, UniProtDocument> joinUniRef(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD,
            JavaPairRDD<String, MappedUniRef> unirefJavaPair) {

        return uniProtDocumentRDD
                .leftOuterJoin(unirefJavaPair)
                .mapValues(new UniRefMappedToUniprotDocument());
    }

    /**
     * @param uniProtEntryRDD JavaPairRDD<accesion, UniProtKBEntry>
     * @return RDD of JavaPairRDD<accesion, UniProtKBEntry> with extended GoEvidence mapped
     *     information
     */
    private JavaPairRDD<String, UniProtKBEntry> joinGoEvidences(
            JavaPairRDD<String, UniProtKBEntry> uniProtEntryRDD) {
        JavaPairRDD<String, Iterable<GOEvidence>> goEvidenceRDD =
                GOEvidencesRDDReader.load(parameter);
        return uniProtEntryRDD.leftOuterJoin(goEvidenceRDD).mapValues(new GOEvidenceMapper());
    }

    /**
     * @param uniProtDocumentRDD current JavaPairRDD<accesion, UniProtDocument>
     * @return JavaPairRDD<accesion, UniProtDocument> with mapped PIR Computationally mapped pubmed
     *     ids
     */
    private JavaPairRDD<String, UniProtDocument> joinLiteratureMapped(
            JavaPairRDD<String, UniProtDocument> uniProtDocumentRDD) {
        JavaPairRDD<String, Iterable<String>> literatureMappedRDD =
                LiteratureMappedRDDReader.loadAccessionPubMedRDD(parameter);
        return uniProtDocumentRDD
                .leftOuterJoin(literatureMappedRDD)
                .mapValues(new LiteratureMappedToUniProtDocument());
    }
}
