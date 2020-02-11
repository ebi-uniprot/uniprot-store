package org.uniprot.store.spark.indexer.uniprot.mapper.model;

import java.io.Serializable;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.uniprot.core.uniref.UniRefType;

/**
 * Model used to Map UniRef data into UniprotDocument
 *
 * @author lgonzales
 * @since 2019-10-21
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class MappedUniRef implements Serializable {

    private static final long serialVersionUID = 2652538177977809226L;
    private String clusterID;
    private UniRefType uniRefType;
    private String uniparcUPI;
}
