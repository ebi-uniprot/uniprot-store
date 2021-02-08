package org.uniprot.store.spark.indexer.uniparc.mapper;

import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.util.Utils;

import scala.Tuple2;

/**
 * This class get an UniParcEntry and extract all taxonomy ids from the entry and returns a List of
 * Tuple{key=taxId,value=uniparcId} so this tuple can be Joined with Taxonomy data
 *
 * @author lgonzales
 * @since 2020-02-20
 */
public class UniParcTaxonomyMapper implements PairFlatMapFunction<UniParcEntry, String, String> {

    private static final long serialVersionUID = -2088597839162412239L;

    @Override
    public Iterator<Tuple2<String, String>> call(UniParcEntry uniParcEntry) throws Exception {
        return uniParcEntry.getUniParcCrossReferences().stream()
                .filter(xref -> Utils.notNull(xref.getOrganism()))
                .map(UniParcCrossReference::getOrganism)
                .map(Organism::getTaxonId)
                .map(String::valueOf)
                .map(taxId -> new Tuple2<>(taxId, uniParcEntry.getUniParcId().getValue()))
                .collect(Collectors.toList())
                .iterator();
    }
}
