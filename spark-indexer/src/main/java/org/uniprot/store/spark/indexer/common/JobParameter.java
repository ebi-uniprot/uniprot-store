package org.uniprot.store.spark.indexer.common;

import java.util.ResourceBundle;

import lombok.Builder;
import lombok.Getter;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
@Getter
@Builder
public class JobParameter {

    private volatile JavaSparkContext sparkContext;

    private final String releaseName;

    private final ResourceBundle applicationConfig;
}
