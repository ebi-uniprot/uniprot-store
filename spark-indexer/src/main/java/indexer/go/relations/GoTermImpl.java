package indexer.go.relations;

import java.util.HashSet;
import java.util.Set;

/**
 * Model Implementation for Go Terms Relations
 *
 * @author lgonzales
 * @since 2019-11-08
 */
public class GoTermImpl implements GoTerm {

    private static final long serialVersionUID = -2969929999892986987L;
    private final String goId;
    private final String name;
    private Set<GoTerm> ancestors;

    public GoTermImpl(String goId, String name) {
        this.goId = goId;
        this.name = name;
        this.ancestors = new HashSet<>();
    }

    public GoTermImpl(String goId, String name, Set<GoTerm> ancestors) {
        this(goId, name);
        this.ancestors.addAll(ancestors);
    }

    @Override
    public String getId() {
        return goId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<GoTerm> getAncestors() {
        return ancestors;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((goId == null) ? 0 : goId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        GoTermImpl other = (GoTermImpl) obj;
        if (goId == null) {
            if (other.goId != null) return false;
        } else if (!goId.equals(other.goId)) return false;
        return true;
    }

    @Override
    public int compareTo(GoTerm g1) {
        return g1.getId().compareTo(this.getId());
    }
}
