package uk.ac.ebi.uniprot.indexer.uniprotkb;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.go.GoTermFileReader;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.keyword.KeywordRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayFileRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.pathway.PathwayRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.FileNodeIterable;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyMapRepo;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;

import java.io.File;

/**
 * Created 10/04/19
 *
 * @author Edd
 */
@Configuration
@EnableConfigurationProperties({UniProtKBIndexingProperties.class})
public class UniProtKBConfig {
    private UniProtKBIndexingProperties uniProtKBIndexingProperties = new UniProtKBIndexingProperties();

    @Bean
    ItemReader<UniProtEntry> entryItemReader() {
        return new UniProtEntryItemReader(uniProtKBIndexingProperties);
    }

    @Bean
    ItemProcessor<UniProtEntry, UniProtDocument> uniProtDocumentItemProcessor() {
        return new UniProtEntryProcessor(createTaxonomyRepo(),
                                         createGoRelationRepo(),
                                         createKeywordRepo(),
                                         createPathwayRepo());
    }

    @Bean
    UniProtKBIndexingProperties indexingProperties() {
        return uniProtKBIndexingProperties;
    }

    private PathwayRepo createPathwayRepo() {
        return new PathwayFileRepo(uniProtKBIndexingProperties.getPathwayFile());
    }

    private KeywordRepo createKeywordRepo() {
        return new KeywordFileRepo(uniProtKBIndexingProperties.getKeywordFile());
    }

    private GoRelationRepo createGoRelationRepo() {
        return new GoRelationFileRepo(
                new GoRelationFileReader(uniProtKBIndexingProperties.getGoRelationsFile()),
                new GoTermFileReader(uniProtKBIndexingProperties.getGoFile()));
    }

    private TaxonomyRepo createTaxonomyRepo() {
        return new TaxonomyMapRepo(new FileNodeIterable(new File(uniProtKBIndexingProperties.getTaxonomyFile())));
    }
}
