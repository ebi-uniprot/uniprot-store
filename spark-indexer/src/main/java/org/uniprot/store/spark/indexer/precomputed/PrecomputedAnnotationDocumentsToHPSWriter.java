package org.uniprot.store.spark.indexer.precomputed;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.precomputed.mapper.PrecomputedAnnotationEntryToDocumentMapper;
import org.uniprot.store.spark.indexer.uniprot.PrecomputedAnnotationRDDReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrecomputedAnnotationDocumentsToHPSWriter implements DocumentsToHPSWriter {

    private final JobParameter jobParameter;
    private final PrecomputedAnnotationRDDReader precomputedAnnotationRDDReader;

    public PrecomputedAnnotationDocumentsToHPSWriter(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.precomputedAnnotationRDDReader = new PrecomputedAnnotationRDDReader(jobParameter);
    }

    @Override
    public void writeIndexDocumentsToHPS() {
        JavaRDD<PrecomputedAnnotationDocument> documents = loadDocuments();
        saveToHPS(documents);
        log.info("Completed writing precomputed annotation documents to HPS");
    }

    JavaRDD<PrecomputedAnnotationDocument> loadDocuments() {
        JavaRDD<UniProtKBEntry> precomputedAnnotationEntryRDD =
                precomputedAnnotationRDDReader.load();
        return precomputedAnnotationEntryRDD.map(new PrecomputedAnnotationEntryToDocumentMapper());
    }

    void saveToHPS(JavaRDD<PrecomputedAnnotationDocument> documentRDD) {
        String hpsPath =
                getCollectionOutputReleaseDirPath(
                        jobParameter.getApplicationConfig(),
                        jobParameter.getReleaseName(),
                        SolrCollection.precomputedannotation);
        SolrUtils.saveSolrInputDocumentRDD(documentRDD, hpsPath);
    }
}
