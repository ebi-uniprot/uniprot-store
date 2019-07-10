package uk.ac.ebi.uniprot.indexer.crossref.writers;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author sahmad
 */
public class CrossRefUniProtCountWriter implements ItemWriter<CrossRefDocument> {
    private static final String ACCESSION_STR = "accession";
    private static final String REVIEWED_PROTEIN_COUNT_STR = "reviewed_protein_count";
    private static final String UNREVIEWED_PROTEIN_COUNT_STR = "unreviewed_protein_count";

    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;

    public CrossRefUniProtCountWriter(UniProtSolrOperations solrOperations, SolrCollection collection) {
        this.solrOperations = solrOperations;
        this.collection = collection;
    }

    @Override
    public void write(List<? extends CrossRefDocument> items) {
        for (CrossRefDocument document : items) {
            SolrInputDocument solrInputDocument = new SolrInputDocument();

            // reviewed protein count
            Map<String, Object> revProtField = new HashMap<>(1);
            revProtField.put("set", document.getReviewedProteinCount());
            solrInputDocument.addField(REVIEWED_PROTEIN_COUNT_STR, revProtField);

            // unreviewed protein count
            Map<String, Object> unrevProtField = new HashMap<>(1);
            unrevProtField.put("set", document.getUnreviewedProteinCount());
            solrInputDocument.addField(UNREVIEWED_PROTEIN_COUNT_STR, unrevProtField);

            solrInputDocument.addField(ACCESSION_STR, document.getAccession());
            this.solrOperations.saveBean(collection.name(), solrInputDocument);
        }

        this.solrOperations.softCommit(collection.name());
    }
}
