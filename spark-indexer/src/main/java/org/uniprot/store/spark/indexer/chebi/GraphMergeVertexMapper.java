package org.uniprot.store.spark.indexer.chebi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;
import org.uniprot.core.util.Utils;

import scala.Function2;

class GraphMergeVertexMapper
        implements Function2<ChebiEntry, ChebiEntry, ChebiEntry>, Serializable {
    private static final long serialVersionUID = -8568735208735890731L;

    @Override
    public ChebiEntry apply(ChebiEntry n1, ChebiEntry n2) {
        List<ChebiEntry> relatedIds = n1.getRelatedIds();
        relatedIds.addAll(n2.getRelatedIds());

        Collection<ChebiEntry> mergedIds =
                relatedIds.stream()
                        .collect(
                                Collectors.toMap(
                                        ChebiEntry::getId, Function.identity(), this::mergeChebi))
                        .values();
        relatedIds = new ArrayList<>(mergedIds);

        return ChebiEntryBuilder.from(n1).relatedIdsSet(relatedIds).build();
    }

    private ChebiEntry mergeChebi(ChebiEntry chebi1, ChebiEntry chebi2) {
        ChebiEntryBuilder mergedBuilder = ChebiEntryBuilder.from(chebi1);
        if (Utils.nullOrEmpty(chebi1.getName())) {
            mergedBuilder.name(chebi2.getName());
        }
        if (Utils.nullOrEmpty(chebi1.getInchiKey())) {
            mergedBuilder.inchiKey(chebi2.getInchiKey());
        }
        if (Utils.nullOrEmpty(chebi1.getRelatedIds())) {
            mergedBuilder.relatedIdsSet(chebi2.getRelatedIds());
        }
        return mergedBuilder.build();
    }
}
