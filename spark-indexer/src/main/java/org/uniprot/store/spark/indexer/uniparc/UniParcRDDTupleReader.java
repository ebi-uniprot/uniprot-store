package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryConverter;

/**
 * Responsible to Load JavaRDD{UniParcEntry}
 *
 * @author lgonzales
 * @since 2020-02-13
 */
public class UniParcRDDTupleReader extends BaseUniParcRDDTupleReader<UniParcEntry> {

    public UniParcRDDTupleReader(JobParameter jobParameter, boolean shouldRepartition) {
        super(jobParameter, shouldRepartition);
    }

    @Override
    protected Encoder<UniParcEntry> getEncoder() {
        Encoder<UniParcEntry> entryEncoder =
                (Encoder<UniParcEntry>) Encoders.kryo(UniParcEntry.class);
        return entryEncoder;
    }

    @Override
    protected MapFunction<Row, UniParcEntry> getConverter() {
        return new DatasetUniParcEntryConverter();
    }
}
