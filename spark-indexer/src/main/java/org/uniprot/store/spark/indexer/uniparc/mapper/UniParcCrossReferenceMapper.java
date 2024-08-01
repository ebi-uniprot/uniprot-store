package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.util.Pair;
import org.uniprot.core.util.PairImpl;

public class UniParcCrossReferenceMapper
        implements FlatMapFunction<UniParcEntry, Pair<String, List<UniParcCrossReference>>> {

    private final int batchSize;

    public UniParcCrossReferenceMapper(int batchSize){
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<Pair<String, List<UniParcCrossReference>>> call(UniParcEntry uniParcEntry)
            throws Exception {
        int batchIndex = 0;
        int xrefIndex = 0;
        String uniParcId = uniParcEntry.getUniParcId().getValue();
        List<UniParcCrossReference> batchItems = new ArrayList<>();
        List<Pair<String, List<UniParcCrossReference>>> result = new ArrayList<>();
        for (UniParcCrossReference xref: uniParcEntry.getUniParcCrossReferences()) {
            batchItems.add(xref);
            if(++xrefIndex % batchSize == 0) {
                result.add(new PairImpl<>(uniParcId + "_" + batchIndex++, batchItems));
                batchItems = new ArrayList<>();
            }
        }
        if(!batchItems.isEmpty()){
            result.add(new PairImpl<>(uniParcId + "_" + batchIndex, batchItems));
        }
        return result.iterator();
    }
}