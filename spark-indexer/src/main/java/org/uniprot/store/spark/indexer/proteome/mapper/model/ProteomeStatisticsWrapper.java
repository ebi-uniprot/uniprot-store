package org.uniprot.store.spark.indexer.proteome.mapper.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.uniprot.core.proteome.ProteomeStatistics;

@Getter
@EqualsAndHashCode
@Builder(toBuilder = true)
public class ProteomeStatisticsWrapper {
    private final ProteomeStatistics proteomeStatistics;
}
