package org.uniprot.store.spark.indexer.main.verifiers;

import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.exception.SolrIndexException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.common.writer.SolrIndexParameter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getCollectionOutputReleaseDirPath;
import static org.uniprot.store.spark.indexer.common.util.SparkUtils.getSolrCollection;

/**
 * This is a temporary class to add missing solr documents after the first run index...
 * Basically we are iterating over all output documents and search it in solr.
 * The ones that we do not find, we try to add it again.
 */
@Slf4j
public class VerifyAndAddMissingDocumentsMain {


    public static void main(String[] args) {
        ResourceBundle applicationConfig = SparkUtils.loadApplicationProperty();


        SolrCollection collection = getSolrCollection(args[1]).get(0);
        String hdfsFilePath =
                getCollectionOutputReleaseDirPath(applicationConfig, args[0], collection);
        log.info("Output Documents Path: {}", hdfsFilePath);


        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(applicationConfig)) {
            SolrIndexParameter solrParameter = getSolrIndexParameter(collection, applicationConfig);
            sparkContext.objectFile(hdfsFilePath)
                    .map(obj -> (SolrInputDocument) obj)
                    .foreachPartition(new VerifySolrIndexWriter(solrParameter));
        } catch (Exception e) {
            throw new IndexDataStoreException("Unexpected error during DataStore index", e);
        } finally {
            log.info("All jobs finished!!!");
        }
    }

    static SolrIndexParameter getSolrIndexParameter(
            SolrCollection collection, ResourceBundle config) {
        String delay = config.getString("solr.retry.delay");
        String maxRetry = config.getString("solr.max.retry");
        String batchSize = config.getString("solr.index.batch.size");
        return SolrIndexParameter.builder()
                .collectionName(collection.name())
                .zkHost(config.getString("solr.zkhost"))
                .delay(Long.parseLong(delay))
                .maxRetry(Integer.parseInt(maxRetry))
                .batchSize(Integer.parseInt(batchSize))
                .build();
    }

    private static class VerifySolrIndexWriter implements VoidFunction<Iterator<SolrInputDocument>> {

        private static final long serialVersionUID = -4229642171927549015L;
        private final SolrIndexParameter parameter;

        public VerifySolrIndexWriter(SolrIndexParameter parameter) {
            this.parameter = parameter;
        }

        @Override
        public void call(Iterator<SolrInputDocument> docs) throws Exception {
            try (SolrClient client = getSolrClient()) {
                BatchIterable iterable = new BatchIterable(docs, parameter.getBatchSize());
                int idCount = 0;
                int foundIdCount = 0;
                for (Collection<SolrInputDocument> batch : iterable) {
                    Collection<String> ids = batch.stream().map(this::getAccessionId).collect(Collectors.toList());
                    idCount += ids.size();
                    List<String> foundIds = getByIds(client, ids);
                    foundIdCount += foundIds.size();
                    ids.removeAll(foundIds);
                    if(!ids.isEmpty()) {
                        Collection<SolrInputDocument> needAdd = batch.stream()
                                .filter(doc -> {
                                    String batchDocId = getAccessionId(doc);
                                    return !ids.contains(batchDocId);
                                }).collect(Collectors.toList());
                        log.info("ADDING DOCUMENTS: " + needAdd.stream().map(this::getAccessionId).collect(Collectors.joining(",")));
                        client.add(parameter.getCollectionName(), needAdd);
                        client.commit(parameter.getCollectionName());
                    }
                }
                log.warn("IDS:FOUND:"+ (idCount == foundIdCount) +" : " + idCount + " "+ foundIdCount);
            } catch (Exception e) {
                String errorMessage =
                        "Exception indexing data to Solr, for collection "
                                + parameter.getCollectionName();
                throw new SolrIndexException(errorMessage, e);
            }
        }

        private List<String> getByIds(SolrClient client, Collection<String> ids) throws SolrServerException, IOException {
            ModifiableSolrParams param = new ModifiableSolrParams();
            param.set("fl", "accession_id");
            SolrDocumentList foundDocs = client.getById(parameter.getCollectionName(), ids, param);
            return foundDocs.stream()
                    .map(doc -> doc.get("accession_id").toString())
                    .collect(Collectors.toList());
        }

        private String getAccessionId(SolrInputDocument doc) {
            return doc.get("accession_id").getFirstValue().toString();
        }

        protected SolrClient getSolrClient() {
            return new CloudSolrClient.Builder(singletonList(parameter.getZkHost()), Optional.empty())
                    .build();
        }
    }

    private static class BatchIterable implements Iterable<Collection<SolrInputDocument>> {
        private final Iterator<SolrInputDocument> sourceIterator;
        private final int batchSize;

        public BatchIterable(Iterator<SolrInputDocument> sourceIterator, int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException(
                        "Batch size must be bigger than 1. Current value is:" + batchSize);
            }
            this.batchSize = batchSize;
            this.sourceIterator = sourceIterator;
        }

        @Override
        public Iterator<Collection<SolrInputDocument>> iterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return sourceIterator.hasNext();
                }

                @Override
                public List<SolrInputDocument> next() {
                    List<SolrInputDocument> batch = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        if (sourceIterator.hasNext()) {
                            batch.add(sourceIterator.next());
                        } else {
                            break;
                        }
                    }
                    return batch;
                }
            };
        }
    }
}
