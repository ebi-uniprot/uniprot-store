package org.uniprot.store.spark.indexer.taxonomy.mapper.model;

import java.io.Serializable;

import org.uniprot.core.taxonomy.TaxonomyStatistics;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
public class TaxonomyStatisticsWrapper implements Serializable {

    private static final long serialVersionUID = 331034812215045938L;
    private final TaxonomyStatistics statistics;

    private final boolean organismReviewedProtein;

    private final boolean organismUnreviewedProtein;
}
