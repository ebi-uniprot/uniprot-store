package uk.ac.ebi.uniprot.writers;

import org.apache.solr.client.solrj.SolrClient;
import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.models.DBXRef;

import java.util.List;

public class DBXRefWriter implements ItemWriter<DBXRef> {
    private SolrClient solrClient;

    public DBXRefWriter(SolrClient solrClient){
        this.solrClient = solrClient;
    }

    @Override
    public void write(List<? extends DBXRef> items) throws Exception {
        this.solrClient.addBeans(items);
        this.solrClient.commit();

    }
}
