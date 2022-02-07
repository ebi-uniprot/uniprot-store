package org.uniprot.store.spark.indexer.rhea.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@AllArgsConstructor
@Builder(toBuilder = true)
@Getter
public class RheaComp implements Serializable {

    private static final long serialVersionUID = 3446015492963061139L;

    private String id;
    private String name;
}
