package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@ConfigurationProperties(prefix = "uniprotkb.indexing")
public class UniProtKBConfig {
    // TODO: 10/04/19 to contain beans such as itemprocessor (i.e., doc converter), reader, etc.

    // TODO: 10/04/19 beans for files required by entry iterator

    private UniProtKBIndexingProperties uniProtKBIndexingProperties = new UniProtKBIndexingProperties();

    @Bean
    ItemReader<UniProtEntry> entryItemReader() {
        return null;
    }

    @Bean
    ItemProcessor<UniProtEntry, UniProtDocument> uniProtDocumentItemProcessor() {
        return null;
    }

    @Bean
    UniProtKBIndexingProperties indexingProperties() {
        return uniProtKBIndexingProperties;
    }
}
