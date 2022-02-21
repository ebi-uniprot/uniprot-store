package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serializable;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * @author sahmad
 * @created 08/02/2022
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class SubcellularIdValue implements Serializable {
    private static final long serialVersionUID = -7882130165172382886L;
    private String id;
    private String value;
}
