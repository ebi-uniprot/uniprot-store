package org.uniprot.store.spark.indexer.chebi;

import java.io.Serializable;

import org.uniprot.core.cv.chebi.ChebiEntry;

import scala.Function3;

class GraphVerticesProgramMapper
        implements Function3<Object, ChebiEntry, ChebiEntry, ChebiEntry>, Serializable {
    private static final long serialVersionUID = 129225055353000743L;

    @Override
    public ChebiEntry apply(Object l, ChebiEntry treeNodeThis, ChebiEntry treeNodeIn) {
        if (treeNodeIn != null) {
            return treeNodeIn;
        } else {
            return treeNodeThis;
        }
    }
}
