package uk.ac.ebi.uniprot.steps;


import org.apache.solr.client.solrj.SolrClient;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.models.DBXRef;
import uk.ac.ebi.uniprot.readers.*;
import uk.ac.ebi.uniprot.utils.Constants;
import uk.ac.ebi.uniprot.writers.*;

import java.io.IOException;

@Configuration
public class IndexCrossRefStep {
    @Autowired
    private StepBuilderFactory steps;
    @Autowired
    private SolrClient solrClient;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;
    @Value(("${indexer.xref.ftp.url}"))
    private String xrefFTP;

    @Bean
    public Step indexCrossRef(StepExecutionListener stepListener, ItemReader<DBXRef> xrefReader, ItemWriter<DBXRef> xrefWriter){
        return this.steps.get(Constants.CROSS_REF_INDEX_STEP)
                .<DBXRef, DBXRef>chunk(this.chunkSize)
                .reader(xrefReader)
                .writer(xrefWriter)
                .listener(stepListener)
                .build();
    }

    @Bean
    public ItemReader<DBXRef> xrefReader() throws IOException {
        return new DBXRefReader(this.xrefFTP);
    }

    @Bean
    public ItemWriter<DBXRef> xrefWriter(){
        return new DBXRefWriter(this.solrClient);
    }
}
