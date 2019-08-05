package org.uniprot.store.indexer.common.config;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Configuration;

/**
 * Caching configuration. Beans can be added to this class, but its key purpose
 * is to ensure the {@code @EnableCaching} annotation is in place.
 * <p>
 * Created 05/07/19
 *
 * @author Edd
 */
@Configuration
@EnableCaching
public class CacheConfig {
}