package org.uniprot.store.spark.indexer.literature;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.PairRDDReader;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureEntryAggregationMapper;
import org.uniprot.store.spark.indexer.literature.mapper.LiteratureEntryUniProtKBMapper;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

public class LiteratureUniProtKBRDDReader implements PairRDDReader<String, LiteratureEntry> {

    private final JobParameter jobParameter;

    public LiteratureUniProtKBRDDReader(JobParameter jobParameter) {
        this.jobParameter = jobParameter;
    }

    @Override
    public JavaPairRDD<String, LiteratureEntry> load() {
        UniProtKBRDDTupleReader uniProtKBReader =
                new UniProtKBRDDTupleReader(this.jobParameter, false);
        JavaRDD<String> uniProtKBEntryStringsRDD = uniProtKBReader.loadFlatFileToRDD();

        return uniProtKBEntryStringsRDD
                .flatMapToPair(new LiteratureEntryUniProtKBMapper())
                .aggregateByKey(
                        null,
                        new LiteratureEntryAggregationMapper(),
                        new LiteratureEntryAggregationMapper());
    }
}
