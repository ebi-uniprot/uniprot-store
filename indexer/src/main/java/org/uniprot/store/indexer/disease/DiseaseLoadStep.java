package org.uniprot.store.indexer.disease;

import java.io.IOException;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.store.indexer.common.config.UniProtSolrOperations;
import org.uniprot.store.indexer.common.utils.Constants;
import org.uniprot.store.indexer.common.writer.SolrDocumentWriter;
import org.uniprot.store.search.SolrCollection;
import org.uniprot.store.search.document.disease.DiseaseDocument;

@Configuration
public class DiseaseLoadStep {

    @Autowired private StepBuilderFactory steps;

    @Autowired private UniProtSolrOperations solrOperations;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.disease.file.path}"))
    private String filePath;

    @Bean(name = "IndexDiseaseStep")
    public Step indexDisease(
            StepExecutionListener stepListener,
            ChunkListener chunkListener,
            @Qualifier("DiseaseReader") ItemReader<DiseaseEntry> diseaseReader,
            @Qualifier("DiseaseProcessor")
                    ItemProcessor<DiseaseEntry, DiseaseDocument> diseaseProcessor,
            @Qualifier("DiseaseWriter") ItemWriter<DiseaseDocument> diseaseWriter) {
        return this.steps
                .get(Constants.DISEASE_INDEX_STEP)
                .<DiseaseEntry, DiseaseDocument>chunk(this.chunkSize)
                .reader(diseaseReader)
                .processor(diseaseProcessor)
                .writer(diseaseWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "DiseaseReader")
    public ItemReader<DiseaseEntry> diseaseReader() throws IOException {
        return new DiseaseItemReader(this.filePath);
    }

    @Bean(name = "DiseaseWriter")
    public ItemWriter<DiseaseDocument> diseaseWriter() {
        return new SolrDocumentWriter<>(this.solrOperations, SolrCollection.disease);
    }

    @Bean(name = "DiseaseProcessor")
    public ItemProcessor<DiseaseEntry, DiseaseDocument> diseaseProcessor() {

        return new DiseaseProcessor();
    }
}
