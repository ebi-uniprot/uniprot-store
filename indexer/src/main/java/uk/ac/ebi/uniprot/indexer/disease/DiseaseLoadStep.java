package uk.ac.ebi.uniprot.indexer.disease;


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
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.cv.disease.Disease;
import uk.ac.ebi.uniprot.indexer.common.utils.Constants;
import uk.ac.ebi.uniprot.indexer.common.writer.SolrDocumentWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.disease.DiseaseDocument;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

@Configuration
public class DiseaseLoadStep {

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private SolrTemplate solrTemplate;

    @Value(("${ds.import.chunk.size}"))
    private Integer chunkSize;

    @Value(("${indexer.disease.file.path}"))
    private String filePath;

    @Bean(name = "IndexDiseaseStep")
    public Step indexDisease(StepExecutionListener stepListener, ChunkListener chunkListener,
                             @Qualifier("DiseaseReader") ItemReader<Disease> diseaseReader,
                             @Qualifier("DiseaseProcessor") ItemProcessor<Disease, DiseaseDocument> diseaseProcessor,
                             @Qualifier("DiseaseWriter") ItemWriter<DiseaseDocument> diseaseWriter) {
        return this.steps.get(Constants.DISEASE_INDEX_STEP)
                .<Disease, DiseaseDocument>chunk(this.chunkSize)
                .reader(diseaseReader)
                .processor(diseaseProcessor)
                .writer(diseaseWriter)
                .listener(stepListener)
                .listener(chunkListener)
                .build();
    }

    @Bean(name = "DiseaseReader")
    public ItemReader<Disease> diseaseReader() throws IOException {
        return new DiseaseItemReader(this.filePath);
    }

    @Bean(name = "DiseaseWriter")
    public ItemWriter<DiseaseDocument> diseaseWriter() {
        return new SolrDocumentWriter<>(this.solrTemplate, SolrCollection.disease);
    }

    @Bean(name = "DiseaseProcessor")
    public ItemProcessor<Disease, DiseaseDocument> diseaseProcessor(@Qualifier("readDataSource") DataSource readDataSource)
            throws SQLException {

        return new DiseaseProcessor(readDataSource);

    }

}
