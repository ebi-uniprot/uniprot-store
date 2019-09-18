package org.uniprot.store.datastore.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.uniprot.store.datastore.common.config.StoreProperties;

/**
 *
 * @author jluo
 * @date: 15 Aug 2019
 *
*/
@TestConfiguration
@ConfigurationProperties(prefix = "store.uniprotkb")
public class StoreTestConfig {
	@Autowired	
	private StoreProperties properties;

	public StoreTestConfig(StoreProperties properties) {
		this.properties = properties;
	}
	
	
}

