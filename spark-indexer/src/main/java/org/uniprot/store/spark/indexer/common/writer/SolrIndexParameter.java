package org.uniprot.store.spark.indexer.common.writer;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;

/**
 * @author lgonzales
 * @since 02/12/2020
 */
@Getter
@Builder
public class SolrIndexParameter implements Serializable {

    private static final long serialVersionUID = 6295298619317181349L;
    private final String zkHost;
    private final String collectionName;
    private final long delay;
    private final int maxRetry;
    private final int batchSize;
}
