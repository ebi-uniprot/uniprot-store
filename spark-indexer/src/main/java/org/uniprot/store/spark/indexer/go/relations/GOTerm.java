package org.uniprot.store.spark.indexer.go.relations;

import java.io.Serializable;
import java.util.Set;

/**
 * Model for GO Terms Relations
 *
 * @author lgonzales
 * @since 2019-10-25
 */
public interface GOTerm extends Serializable, Comparable<GOTerm> {

    String getId();

    String getName();

    Set<GOTerm> getAncestors();
}
