package org.uniprot.store.spark.indexer.go.evidence;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.uniprot.core.uniprotkb.evidence.Evidence;

/**
 * Model class for the mapped GO Evidence
 *
 * @author lgonzales
 * @since 2019-10-21
 */
@Getter
@ToString
@EqualsAndHashCode
public class GOEvidence implements Serializable {

    private static final long serialVersionUID = 5783511629897468712L;

    private String goId;

    private Evidence evidence;

    public GOEvidence(String goId, Evidence evidence) {
        this.goId = goId;
        this.evidence = evidence;
    }
}
