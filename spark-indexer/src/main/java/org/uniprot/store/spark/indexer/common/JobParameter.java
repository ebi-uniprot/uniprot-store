package org.uniprot.store.spark.indexer.common;

import org.apache.spark.api.java.JavaSparkContext;

import com.typesafe.config.Config;

import lombok.Builder;
import lombok.Getter;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
@Getter
@Builder
public class JobParameter {

    private final JavaSparkContext sparkContext;

    private final String releaseName;

    private final TaxDb taxDb;

    private final Config applicationConfig;
}
