package org.uniprot.store.config.model;

/**
 * Created 05/03/20
 *
 * @author Edd
 */
public interface Field {
    String getId();

    Integer getSeqNumber();

    String getParentId();

    Integer getChildNumber();
}
