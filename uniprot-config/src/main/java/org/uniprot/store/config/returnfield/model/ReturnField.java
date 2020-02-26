package org.uniprot.store.config.returnfield.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lgonzales
 * @since 2020-02-25
 */
@Data
public class ReturnField implements Serializable {
    private static final long serialVersionUID = 606080616718758299L;

    private String name;
    private String label;
    private String path;
    private String filter;

}
