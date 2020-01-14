package org.uniprot.store.spark.indexer.go.relations;

import java.util.HashSet;
import java.util.Set;

import org.uniprot.core.util.Utils;

/**
 * Model Implementation for GO Terms Relations
 *
 * @author lgonzales
 * @since 2019-11-08
 */
public class GOTermImpl implements GOTerm {

    private static final long serialVersionUID = -2969929999892986987L;
    private final String goId;
    private final String name;
    private Set<GOTerm> ancestors;

    public GOTermImpl(String goId, String name) {
        this.goId = goId;
        this.name = name;
        this.ancestors = new HashSet<>();
    }

    public GOTermImpl(String goId, String name, Set<GOTerm> ancestors) {
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
    public Set<GOTerm> getAncestors() {
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
        GOTermImpl other = (GOTermImpl) obj;
        if (goId == null) {
            if (Utils.notNull(other.goId)) return false;
        } else if (!goId.equals(other.goId)) return false;
        return true;
    }

    @Override
    public int compareTo(GOTerm g1) {
        return g1.getId().compareTo(this.getId());
    }
}
