package org.uniprot.store.datastore.uniref.config;

import static java.util.Collections.singletonList;

import java.time.temporal.ChronoUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.UniProtStoreClient;
import org.uniprot.store.datastore.common.config.StoreProperties;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortRemoteUniRefEntryStore;

import net.jodah.failsafe.RetryPolicy;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
*/
@Configuration
@EnableConfigurationProperties({UniRefStoreProperties.class })
@Profile("online")
public class UniRefStoreConfig {

	 private final UniRefStoreProperties unirefStoreProperties;

	    @Autowired
	    public UniRefStoreConfig(UniRefStoreProperties unirefStoreProperties) {
	        this.unirefStoreProperties = unirefStoreProperties;
	    }
	    
	    @Bean
	    @ConfigurationProperties(prefix = "store.uniref")
	    public StoreProperties unirefStoreProperties() {
	        return new StoreProperties();
	    }

	    @Bean
	    public UniProtStoreClient<UniRefEntry> unirefStoreClient() {
	        VoldemortClient<UniRefEntry> client = new VoldemortRemoteUniRefEntryStore(
	        		unirefStoreProperties().getNumberOfConnections(),
	        		unirefStoreProperties().getStoreName(),
	        		unirefStoreProperties().getHost());
	        return new UniProtStoreClient<>(client);
	    }
	    
	    @Bean
	    public RetryPolicy<Object> writeRetryPolicy() {
	        return new RetryPolicy<>()
	                .handle(singletonList(Exception.class))
	                .withMaxRetries(unirefStoreProperties.getWriteRetryLimit())
	                .withBackoff(unirefStoreProperties.getWriteRetryBackOffFromMillis(),
	                		unirefStoreProperties.getWriteRetryBackOffToMillis(),
	                             ChronoUnit.MILLIS);
	    }
	    
	  
}

