package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
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

    @Serial
    private static final long serialVersionUID = -3650716308685435024L;
    private final int batchSize;

    public UniParcCrossReferenceMapper(int batchSize){
        this.batchSize = batchSize;
    }

    @Override
    public Iterator<Pair<String, List<UniParcCrossReference>>> call(UniParcEntry uniParcEntry)
            throws Exception {
        List<Pair<String, List<UniParcCrossReference>>> result = new ArrayList<>();
        int crossRefSize = uniParcEntry.getUniParcCrossReferences().size();
        String uniParcId = uniParcEntry.getUniParcId().getValue();
        int batchIndex = 0;
        for (int i = 0; i < crossRefSize; i = i + batchSize) {
            List<UniParcCrossReference> batchItems =
                    uniParcEntry.getUniParcCrossReferences()
                            .subList(i, Math.min(i + batchSize, crossRefSize));
            result.add(new PairImpl<>(uniParcId + "_" + batchIndex++, new ArrayList<>(batchItems)));
        }
        return result.iterator();
    }
}