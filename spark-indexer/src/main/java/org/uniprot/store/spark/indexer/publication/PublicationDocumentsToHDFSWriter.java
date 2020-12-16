package org.uniprot.store.spark.indexer.publication;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.publication.mapper.UniProtKBEntryToPublicationDocuments;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

/**
 * @author sahmad
 * @created 15/12/2020
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
        UniProtKBRDDTupleReader uniProtKBReader = new UniProtKBRDDTupleReader(parameter, false);
        JavaPairRDD<String, UniProtKBEntry> uniProtKBRDD = uniProtKBReader.load();
        JavaRDD<PublicationDocument> publicationDocumentsRDD =
                uniProtKBRDD.flatMapValues(new UniProtKBEntryToPublicationDocuments()).values();
        saveToHDFS(publicationDocumentsRDD);
        log.info("Completed UniProtKB publication document prepare Solr index");
    }

    void saveToHDFS(JavaRDD<PublicationDocument> publicationDocumentsRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(config, releaseName, SolrCollection.publication);
        SolrUtils.saveSolrInputDocumentRDD(publicationDocumentsRDD, hdfsPath);
    }
}
