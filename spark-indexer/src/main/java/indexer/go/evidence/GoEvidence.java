package indexer.go.evidence;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.uniprot.core.uniprot.evidence.Evidence;

import java.io.Serializable;

/**
 * @author lgonzales
 * @since 2019-10-21
 */
@Getter
@ToString
@EqualsAndHashCode
public class GoEvidence implements Serializable {

    private static final long serialVersionUID = 5783511629897468712L;

    private String goId;

    private Evidence evidence;


    public GoEvidence(String goId, Evidence evidence) {
        this.goId = goId;
        this.evidence = evidence;
    }
}
