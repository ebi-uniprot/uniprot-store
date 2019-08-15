package org.uniprot.store.datastore.uniref;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.uniprotkb.config.AsyncConfig;
import org.uniprot.store.datastore.uniref.config.UniRefStoreConfig;
import org.uniprot.store.datastore.uniref.config.UniRefStoreProperties;
import org.uniprot.store.job.common.listener.LogRateListener;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import net.jodah.failsafe.RetryPolicy;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
*/
@Configuration
@Import({UniRefStoreConfig.class, AsyncConfig.class})
public class UniRefStoreStep {
	private final StepBuilderFactory stepBuilderFactory;
    private final UniRefStoreProperties unirefStoreProperties;

    @Autowired
    public UniRefStoreStep(StepBuilderFactory stepBuilderFactory,
    		UniRefStoreProperties unirefStoreProperties) {
        this.stepBuilderFactory = stepBuilderFactory;
        this.unirefStoreProperties = unirefStoreProperties;
    }
    
    
    // ---------------------- Readers ----------------------
    @Bean
    public ItemReader<Entry> unirefEntryItemReader() {
        return new UniRefXmlEntryReader(unirefStoreProperties.getXmlFilePath());
    }
    
    // ---------------------- Writers ----------------------
    @Bean
    public ItemRetryWriter<Entry, UniRefEntry> unirefEntryItemWriter(UniProtStoreClient<UniRefEntry> unirefStoreClient,
                                                           RetryPolicy<Object> writeRetryPolicy) {
        return new UniRefEntryRetryWriter(entries -> entries.forEach(unirefStoreClient::saveEntry),
                                           writeRetryPolicy);
    }
    
    // ---------------------- Listeners ----------------------
    @Bean(name = "uniref")
    public LogRateListener<UniProtEntry> uniProtKBLogRateListener() {
        return new LogRateListener<>(unirefStoreProperties.getLogRateInterval());
    }

}

