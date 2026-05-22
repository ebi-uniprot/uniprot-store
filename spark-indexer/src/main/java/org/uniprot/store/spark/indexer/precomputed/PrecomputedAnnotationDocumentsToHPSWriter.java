package org.uniprot.store.spark.indexer.precomputed;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHPSWriter;
import org.uniprot.store.spark.indexer.precomputed.mapper.PrecomputedAnnotationDocumentProteomeJoin;
import org.uniprot.store.spark.indexer.precomputed.mapper.PrecomputedAnnotationEntryToDocumentMapper;
import org.uniprot.store.spark.indexer.proteome.ProteomeRDDReader;
import org.uniprot.store.spark.indexer.uniprot.PrecomputedAnnotationRDDReader;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
public class PrecomputedAnnotationDocumentsToHPSWriter implements DocumentsToHPSWriter {

    private final JobParameter jobParameter;
    private final PrecomputedAnnotationRDDReader precomputedAnnotationRDDReader;
    private final ProteomeRDDReader proteomeRDDReader;

    public PrecomputedAnnotationDocumentsToHPSWriter(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
        this.precomputedAnnotationRDDReader = new PrecomputedAnnotationRDDReader(jobParameter);
        this.proteomeRDDReader = new ProteomeRDDReader(jobParameter, false);
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
        JavaRDD<PrecomputedAnnotationDocument> precomputedAnnotationDocumentRDD =
                precomputedAnnotationEntryRDD.map(new PrecomputedAnnotationEntryToDocumentMapper());
        //<taxonomyId, precomputedAnnotation>
        JavaPairRDD<Integer, PrecomputedAnnotationDocument> precomputedAnnotationEntryPairedRDD =
                precomputedAnnotationDocumentRDD.mapToPair(
                        doc -> new Tuple2<>(doc.getTaxonomyId(), doc));
        //<taxonomyId, Iterable<proteomeId>>
        JavaPairRDD<Integer, Iterable<String>> taxonomyProteomeIds =
                loadTaxonomyProteomeIds().groupByKey();

        return precomputedAnnotationEntryPairedRDD
                .leftOuterJoin(taxonomyProteomeIds)
                .values()
                .map(new PrecomputedAnnotationDocumentProteomeJoin());
    }

    JavaPairRDD<Integer, String> loadTaxonomyProteomeIds() {
        return proteomeRDDReader.load().values().mapToPair(getProteomeEntryToTaxonomyProteomeId());
    }

    PairFunction<ProteomeEntry, Integer, String> getProteomeEntryToTaxonomyProteomeId() {
        return proteomeEntry ->
                new Tuple2<>(
                        (int) (proteomeEntry.getTaxonomy().getTaxonId()),
                        proteomeEntry.getId().getValue());
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
