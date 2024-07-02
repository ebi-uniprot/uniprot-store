package org.uniprot.store.spark.indexer.uniparc.converter;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

import org.uniprot.core.uniparc.UniParcCrossReference;

public class UniParcCrossReferenceWrapper implements Serializable {
    @Serial private static final long serialVersionUID = -1938416315943584792L;
    private final String id;

    private final UniParcCrossReference uniParcCrossReference;

    public String getId() {
        return id;
    }

    public UniParcCrossReference getUniParcCrossReference() {
        return uniParcCrossReference;
    }

    public UniParcCrossReferenceWrapper(String id, UniParcCrossReference uniParcCrossReference) {
        this.id = id;
        this.uniParcCrossReference = uniParcCrossReference;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniParcCrossReferenceWrapper that = (UniParcCrossReferenceWrapper) o;
        return Objects.equals(id, that.id)
                && Objects.equals(uniParcCrossReference, that.uniParcCrossReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, uniParcCrossReference);
    }
}
