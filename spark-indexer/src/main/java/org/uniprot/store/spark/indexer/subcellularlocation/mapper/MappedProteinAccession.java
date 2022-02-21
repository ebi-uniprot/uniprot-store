package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import java.io.Serializable;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * @author sahmad
 * @created 07/02/2022
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class MappedProteinAccession implements Serializable {
    private static final long serialVersionUID = -4853056776440355078L;
    private String proteinAccession;
    private boolean isReviewed;
}
