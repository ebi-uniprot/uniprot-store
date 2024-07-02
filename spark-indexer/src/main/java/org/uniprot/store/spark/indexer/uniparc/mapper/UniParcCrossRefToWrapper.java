package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.store.spark.indexer.uniparc.converter.UniParcCrossReferenceWrapper;

import scala.Tuple2;

public class UniParcCrossRefToWrapper
        implements FlatMapFunction<
                Tuple2<String, List<UniParcCrossReference>>, UniParcCrossReferenceWrapper> {
    @Serial private static final long serialVersionUID = 6208985996252016652L;

    @Override
    public Iterator<UniParcCrossReferenceWrapper> call(
            Tuple2<String, List<UniParcCrossReference>> tuple) throws Exception {
        // keep xrefId and current repetition count
        Map<String, Integer> xrefIdCount = new HashMap<>();
        String uniParcId = tuple._1;
        List<UniParcCrossReference> xrefs = tuple._2;
        List<UniParcCrossReferenceWrapper> flatList = new ArrayList<>();
        for (UniParcCrossReference xref : xrefs) {
            // get uniparc xref composite key
            String uniParcXrefId = getUniParcXRefId(uniParcId, xref);
            if (xrefIdCount.containsKey(uniParcXrefId)) {
                // add the next suffix from map in case of collision
                String suffixedUniParcXrefId = uniParcXrefId + "-" + xrefIdCount.get(uniParcXrefId);
                xrefIdCount.put(uniParcXrefId, xrefIdCount.get(uniParcXrefId) + 1);
                uniParcXrefId = suffixedUniParcXrefId;
            } else {
                xrefIdCount.put(uniParcXrefId, 1);
            }
            flatList.add(new UniParcCrossReferenceWrapper(uniParcXrefId, xref));
        }
        return flatList.iterator();
    }

    private String getUniParcXRefId(String uniParcId, UniParcCrossReference xref) {
        StringBuilder xrefIdBuilder = new StringBuilder(uniParcId);
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(xref.getDatabase().name());
        xrefIdBuilder.append("-");
        xrefIdBuilder.append(xref.getId());
        return xrefIdBuilder.toString();
    }
}
