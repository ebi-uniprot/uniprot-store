package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.Superkingdom;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.uniprotkb.taxonomy.OrganismName;
import scala.Tuple2;

public class TaxonomyToProteomeEntryMapper implements Function<Tuple2<ProteomeEntry, Optional<TaxonomyEntry>>, ProteomeEntry> {
    private static final long serialVersionUID = 2199750379648037462L;

    @Override
    public ProteomeEntry call(Tuple2<ProteomeEntry, Optional<TaxonomyEntry>> proteomeEntryTaxonomyTuple2) throws Exception {
        ProteomeEntry proteomeEntry = proteomeEntryTaxonomyTuple2._1;
        Optional<TaxonomyEntry> taxonomyEntryOptional = proteomeEntryTaxonomyTuple2._2;

        if (taxonomyEntryOptional.isPresent()) {
            TaxonomyEntry taxonomyEntry = taxonomyEntryOptional.get();
            ProteomeEntryBuilder proteomeEntryBuilder = ProteomeEntryBuilder.from(proteomeEntry);
            proteomeEntryBuilder.taxonomy(taxonomyEntry);
            proteomeEntryBuilder.taxonLineagesSet(taxonomyEntry.getLineages());
            proteomeEntryBuilder.superkingdom(getSuperKingdom(taxonomyEntry));
            return proteomeEntryBuilder.build();
        }

        return proteomeEntry;
    }

    private Superkingdom getSuperKingdom(TaxonomyEntry taxonomyEntry) {
        return taxonomyEntry.getLineages().stream().map(OrganismName::getScientificName)
                .filter(Superkingdom::isSuperkingdom).findFirst().map(Superkingdom::typeOf).orElse(null);
    }
}
