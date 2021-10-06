package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStrain;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStrainBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.Strain;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TaxonomyStrainsJoinMapper  implements Function<Tuple2<TaxonomyEntry, Optional<Iterable<Strain>>>, TaxonomyEntry> {

    private static final long serialVersionUID = 510880357682827997L;

    @Override
    public TaxonomyEntry call(Tuple2<TaxonomyEntry, Optional<Iterable<Strain>>> tuple) throws Exception {
        TaxonomyEntry result = tuple._1;
        if(tuple._2.isPresent()){
            List<TaxonomyStrain> strains = new ArrayList<>();
            StreamSupport.stream(tuple._2.get().spliterator(), false)
                    .collect(Collectors.groupingBy(Strain::getId))
                    .values()
                    .forEach(
                            strainList -> {
                                TaxonomyStrainBuilder builder = new TaxonomyStrainBuilder();
                                for (Strain strain : strainList) {
                                    if (Strain.StrainNameClass.scientific_name.equals(strain.getNameClass())){
                                        builder.name(strain.getName());
                                    } else {
                                        builder.synonymsAdd(strain.getName());
                                    }
                                }
                                strains.add(builder.build());
                            });
            result = TaxonomyEntryBuilder.from(result)
                    .strainsSet(strains)
                    .build();
        }
        return result;
    }

}
