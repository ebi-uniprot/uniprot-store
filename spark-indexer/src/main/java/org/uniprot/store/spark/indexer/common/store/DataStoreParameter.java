package org.uniprot.store.spark.indexer.common.store;

import java.io.Serializable;

import lombok.Builder;
import lombok.Getter;

/**
 * @author lgonzales
 * @since 02/12/2020
 */
@Getter
@Builder
public class DataStoreParameter implements Serializable {

    private static final long serialVersionUID = 2251982818666905541L;

    private final int numberOfConnections;
    private final String storeName;
    private final String connectionURL;
    private final long delay;
    private final int maxRetry;

    private final boolean brotliEnabled;

    private final int brotliLevel;
}
