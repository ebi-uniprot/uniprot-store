package uk.ac.ebi.uniprot.indexer.crossref.steps;


import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.indexer.crossref.readers.CrossRefReader;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.dbxref.CrossRefDocument;

import java.io.IOException;

@Configuration
public class CrossRefStep {
    
    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private SolrTemplate solrTemplate;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.xref.ftp.url}"))
    private String xrefFTP;

    @Bean(name = "IndexCrossRefStep")
    public Step indexCrossRef(StepExecutionListener stepListener, ChunkListener chunkListener,
                              @Qualifier("crossRefReader") ItemReader<CrossRefDocument> xrefReader,
                              @Qualifier("crossRefWriter") ItemWriter<CrossRefDocument> xrefWriter,
                              @Qualifier("crossRefPromotionListener") ExecutionContextPromotionListener promotionListener){
        return this.steps.get(Constants.CROSS_REF_INDEX_STEP)
                .<CrossRefDocument, CrossRefDocument>chunk(this.chunkSize)
                .reader(xrefReader)
                .writer(xrefWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .listener(promotionListener)
                .build();
    }

    @Bean(name = "crossRefReader")
    public ItemReader<CrossRefDocument> xrefReader() throws IOException {
        return new CrossRefReader(this.xrefFTP);
    }

    @Bean(name = "crossRefWriter")
    public ItemWriter<CrossRefDocument> xrefWriter(){
        return new SolrDocumentWriter<>(this.solrTemplate, SolrCollection.crossref);
    }

    @Bean(name = "crossRefPromotionListener")
    public ExecutionContextPromotionListener promotionListener() {
        ExecutionContextPromotionListener executionContextPromotionListener = new ExecutionContextPromotionListener();
        executionContextPromotionListener.setKeys(new String[] {Constants.CROSS_REF_KEY_STR});
        return executionContextPromotionListener;
    }
}
