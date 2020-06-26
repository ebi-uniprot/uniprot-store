package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.common.util.SparkUtils.*;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SolrUtils;
import org.uniprot.store.spark.indexer.common.writer.DocumentsToHDFSWriter;
import org.uniprot.store.spark.indexer.taxonomy.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefTaxonomyJoin;
import org.uniprot.store.spark.indexer.uniref.mapper.UniRefToDocument;

/**
 * This class is responsible to load all the data for UniRefDocument and save it into HDFS
 *
 * @author lgonzales
 * @since 2020-02-07
 */
@Slf4j
public class UniRefDocumentsToHDFSWriter implements DocumentsToHDFSWriter {

    private final JobParameter parameter;

    public UniRefDocumentsToHDFSWriter(JobParameter parameter) {
        this.parameter = parameter;
    }

    @Override
    public void writeIndexDocumentsToHDFS() {
        // JavaPairRDD<taxId,TaxonomyEntry>
        JavaPairRDD<String, TaxonomyEntry> taxonomyEntryJavaPairRDD =
                loadTaxonomyEntryJavaPairRDD();

        // JavaPairRDD<taxId,UniRefDocument>
        UniRefRDDTupleReader reader50 =
                new UniRefRDDTupleReader(UniRefType.UniRef50, parameter, true);
        JavaPairRDD<String, UniRefDocument> uniref50DocRDD =
                reader50.load().mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        UniRefRDDTupleReader reader90 =
                new UniRefRDDTupleReader(UniRefType.UniRef90, parameter, true);
        JavaPairRDD<String, UniRefDocument> uniref90DocRDD =
                reader90.load().mapToPair(new UniRefToDocument());

        // JavaPairRDD<taxId,UniRefDocument>
        UniRefRDDTupleReader reader100 =
                new UniRefRDDTupleReader(UniRefType.UniRef100, parameter, true);
        JavaPairRDD<String, UniRefDocument> uniref100DocRDD =
                reader100.load().mapToPair(new UniRefToDocument());

        JavaRDD<UniRefDocument> unirefDocumentRDD =
                joinTaxonomy(uniref50DocRDD, taxonomyEntryJavaPairRDD)
                        .union(joinTaxonomy(uniref90DocRDD, taxonomyEntryJavaPairRDD))
                        .union(joinTaxonomy(uniref100DocRDD, taxonomyEntryJavaPairRDD));

        saveToHDFS(unirefDocumentRDD);

        log.info("Completed UniRef (100, 90 and 50) prepare Solr index");
    }

    void saveToHDFS(JavaRDD<UniRefDocument> unirefDocumentRDD) {
        String hdfsPath =
                getCollectionOutputReleaseDirPath(
                        parameter.getApplicationConfig(), parameter.getReleaseName(), SolrCollection.uniref);
        SolrUtils.saveSolrInputDocumentRDD(unirefDocumentRDD, hdfsPath);
    }

    JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
        TaxonomyRDDReader taxReader = new TaxonomyRDDReader(parameter, true);
        return taxReader.load();
    }

    private JavaRDD<UniRefDocument> joinTaxonomy(
            JavaPairRDD<String, UniRefDocument> uniRefRDD,
            JavaPairRDD<String, TaxonomyEntry> taxonomyRDD) {
        return uniRefRDD.leftOuterJoin(taxonomyRDD).mapValues(new UniRefTaxonomyJoin()).values();
    }
}
