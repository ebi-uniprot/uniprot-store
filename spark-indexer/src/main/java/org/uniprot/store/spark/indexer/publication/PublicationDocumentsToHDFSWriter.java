package org.uniprot.store.spark.indexer.publication;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.publication.MappedReferenceRDDReader.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.publication.mapper.*;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import com.typesafe.config.Config;

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
    private static final String NO_PUBMED_PREFIX = "NO-PUBMED-";
    private final JobParameter parameter;
    private final Config config;
    private final String releaseName;

    public PublicationDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
        this.config = parameter.getApplicationConfig();
        this.releaseName = parameter.getReleaseName();
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        // load UniProtKB JavaPairRDD<accession_pubMedId, MappedReference>
        JavaPairRDD<String, MappedReference> kbMappedRefsRDD = loadUniProtKBMappedRefs();

        MappedReferenceRDDReader mappedReferenceReader =
                new MappedReferenceRDDReader(parameter, KeyType.ACCESSION_AND_CITATION_ID);
        // load computational JavaPairRDD<accession_pubMedId, MappedReference>
        JavaPairRDD<String, MappedReference> computationalMappedRefsRDD =
                mappedReferenceReader.loadComputationalMappedReference();

        // load community JavaPairRDD<accession_pubMedId, MappedReference>
        JavaPairRDD<String, MappedReference> communityMappedRefsRDD =
                mappedReferenceReader.loadCommunityMappedReference();

        // at this stage there will be duplicated keys
        JavaPairRDD<String, MappedReference> allMappedRefs =
                kbMappedRefsRDD.union(computationalMappedRefsRDD).union(communityMappedRefsRDD);

        // create a document for each pubmed/submission
        // JavaPairRDD<pubMedId, PublicationDocument.Builder>
        JavaPairRDD<String, PublicationDocument.Builder> pubDocRDD =
                allMappedRefs
                        .groupByKey()
                        .mapToPair(new MappedReferencesToPublicationDocumentBuilderConverter());

        // creates
        JavaRDD<PublicationDocument> allDocs =
                pubDocRDD
                        .groupByKey()
                        .flatMap(new IsLargeScalePublicationDocumentFlatMapper())
                        .map(PublicationDocument.Builder::build);

        saveToHDFS(allDocs);

        log.info("Completed writing UniProtKB publication documents to HDFS");
    }

    public static String[] separateJoinKey(String joinKey) {
        String[] parts = joinKey.split("_");
        if (parts[1].startsWith(NO_PUBMED_PREFIX)) {
            parts[1] = null;
        }
        return parts;
    }

    private JavaPairRDD<String, MappedReference> loadUniProtKBMappedRefs() {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.parameter, false);
        JavaRDD<String> uniProtKBEntryStringsRDD = uniProtKBReader.loadFlatFileToRDD();

        return uniProtKBEntryStringsRDD.flatMapToPair(new UniProtKBPublicationToMappedReference());
    }

    void saveToHDFS(JavaRDD<PublicationDocument> publicationDocumentsRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.publication);
        SolrUtils.saveSolrInputDocumentRDD(publicationDocumentsRDD, hdfsPath);
    }
}
