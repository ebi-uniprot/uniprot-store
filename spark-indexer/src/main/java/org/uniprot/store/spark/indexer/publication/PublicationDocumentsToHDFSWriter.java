package org.uniprot.store.spark.indexer.publication;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.indexer.publication.common.PublicationUtils;
import org.uniprot.store.reader.publications.CommunityMappedReferenceConverter;
import org.uniprot.store.reader.publications.ComputationallyMappedReferenceConverter;
import org.uniprot.store.reader.publications.MappedReferenceConverter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.publication.mapper.MappedReferencesToPublicationDocumentConverter;
import org.uniprot.store.spark.indexer.publication.mapper.UniProtKBPublicationToMappedReference;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;
import scala.Tuple2;

import java.util.ResourceBundle;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getInputReleaseDirPath;

/**
 * The purpose of this class is to load all publication sources (UniProtKB references, PIR
 * computationally mapped references, and community mapped publications), join their data to create
 * {@link PublicationDocument}s, and write them to HDFS.
 *
 * <p>Created 19/01/2021
 *
 * @author Edd
 */
@Slf4j
public class PublicationDocumentsToHDFSWriter implements DocumentsToHDFSWriter {
    private final JobParameter parameter;
    private final ResourceBundle config;
    private final String releaseName;

    public PublicationDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        // load UniProtKB mapped references
        JavaPairRDD<String, MappedReference> kbMappedRefsRDD = loadUniProtKBMappedRefs();

        // load computational docs
        JavaPairRDD<String, MappedReference> computationalMappedRefsRDD = loadComputationalDocs();

        // load community docs
        JavaPairRDD<String, MappedReference> communityMappedRefsRDD = loadCommunityDocs();

        // at this stage there will be duplicated keys
        JavaPairRDD<String, MappedReference> allMappedRefs =
                kbMappedRefsRDD.union(computationalMappedRefsRDD).union(communityMappedRefsRDD);

        // group records with identical keys, so that the record value is an iterable
        // of MappedReferences. Then convert this iterable into a PublicationDocument.
        JavaRDD<PublicationDocument> pubDocRDD =
                allMappedRefs
                        .groupByKey()
                        .map(new MappedReferencesToPublicationDocumentConverter());

        saveToHDFS(pubDocRDD);

        log.info("Completed writing UniProtKB publication documents to HDFS");
    }

    public static String getJoinKey(String accession, String pubMed) {
        String refIdentifier = pubMed;
        if (refIdentifier == null) {
            refIdentifier = "NO-PUBMED" + PublicationUtils.getDocumentId();
        }

        return accession + "_" + refIdentifier;
    }

    public static String[] separateJoinKey(String joinKey) {
        String[] parts = joinKey.split("_");
        if (parts[1].startsWith("NO-PUBMED")) {
            parts[1] = null;
        }
        return parts;
    }

    private JavaPairRDD<String, MappedReference> loadUniProtKBMappedRefs() {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        JavaRDD<String> uniProtKBEntryStringsRDD = uniProtKBReader.loadFlatFileToRDD();

        return uniProtKBEntryStringsRDD.flatMapToPair(
                new UniProtKBPublicationToMappedReference());
    }

    private JavaPairRDD<String, MappedReference> loadComputationalDocs() {
        return loadMappedReferenceRDD(
                "computational.mapped.references.file",
                new ComputationallyMappedReferenceConverter());
    }

    private JavaPairRDD<String, MappedReference> loadCommunityDocs() {
        return loadMappedReferenceRDD(
                "community.mapped.references.file", new CommunityMappedReferenceConverter());
    }

    private JavaPairRDD<String, MappedReference> loadMappedReferenceRDD(
            String srcFilePathProperty, MappedReferenceConverter<?> converter) {
        JavaSparkContext jsc = this.parameter.getSparkContext();
        String releaseInputDir = getInputReleaseDirPath(config, this.parameter.getReleaseName());
        String filePath = releaseInputDir + config.getString(srcFilePathProperty);
        JavaRDD<String> rawMappedRefStrRdd = jsc.textFile(filePath);

        return rawMappedRefStrRdd
                .map(converter::convert)
                .mapToPair(
                        ref ->
                                new Tuple2<>(
                                        getJoinKey(
                                                ref.getUniProtKBAccession().getValue(),
                                                ref.getPubMedId()),
                                        ref));
    }

    void saveToHDFS(JavaRDD<PublicationDocument> publicationDocumentsRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.publication);
        SolrUtils.saveSolrInputDocumentRDD(publicationDocumentsRDD, hdfsPath);
    }
}
