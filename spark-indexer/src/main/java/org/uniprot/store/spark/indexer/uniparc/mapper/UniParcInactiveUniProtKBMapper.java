package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.io.Serial;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;

import scala.Tuple2;

public class UniParcInactiveUniProtKBMapper
        implements PairFlatMapFunction<UniParcEntry, String, String> {

    @Serial private static final long serialVersionUID = -4126671332254877952L;

    @Override
    public Iterator<Tuple2<String, String>> call(UniParcEntry uniParcEntry) {
        String uniParcID = uniParcEntry.getUniParcId().getValue();
        List<Tuple2<String, String>> result =
                uniParcEntry.getUniParcCrossReferences().stream()
                        .filter(xref -> !xref.isActive())
                        .filter(UniParcInactiveUniProtKBMapper::isUniProtKBDatabase)
                        .map(xref -> new Tuple2<>(xref.getId(), uniParcID))
                        .toList();
        return result.iterator();
    }

    private static boolean isUniProtKBDatabase(UniParcCrossReference xref) {
        return UniParcDatabase.TREMBL.equals(xref.getDatabase())
                || UniParcDatabase.SWISSPROT.equals(xref.getDatabase())
                || UniParcDatabase.SWISSPROT_VARSPLIC.equals(xref.getDatabase());
    }
}
