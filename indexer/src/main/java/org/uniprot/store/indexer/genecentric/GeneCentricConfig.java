package org.uniprot.store.indexer.genecentric;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.store.indexer.common.config.PeekableResourceAwareItemReader;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 04/11/2020
 */
@Configuration
public class GeneCentricConfig {

    @Bean(name = "geneCentricFlatFileReader")
    public GeneCentricFastaItemReader geneCentricFlatFileReader() {
        FlatFileItemReader<String> flatReader = new FlatFileItemReader<>();
        flatReader.setLineMapper(new PassThroughLineMapper());

        PeekableResourceAwareItemReader<String> peekableItemReader =
                new PeekableResourceAwareItemReader<>();
        peekableItemReader.setDelegate(flatReader);

        GeneCentricFastaItemReader geneCentricReader = new GeneCentricFastaItemReader();
        geneCentricReader.setDelegate(peekableItemReader);
        return geneCentricReader;
    }

    @Bean(name = "geneCentricDocumentConverter")
    public GeneCentricDocumentConverter geneCentricDocumentConverter() {
        ObjectMapper objectMapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();
        return new GeneCentricDocumentConverter(objectMapper);
    }
}
