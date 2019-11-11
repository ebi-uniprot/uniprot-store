package indexer.go.relations;

import java.io.Serializable;
import java.util.Set;

/**
 * @author lgonzales
 * @since 2019-10-25
 */
public interface GoTerm extends Serializable, Comparable<GoTerm> {

    String getId();

    String getName();

    Set<GoTerm> getAncestors();

}
