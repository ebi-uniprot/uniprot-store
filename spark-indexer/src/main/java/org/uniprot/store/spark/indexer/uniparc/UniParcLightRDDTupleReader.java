package org.uniprot.store.spark.indexer.uniparc;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.uniparc.converter.DatasetUniParcEntryLightConverter;

public class UniParcLightRDDTupleReader extends BaseUniParcRDDTupleReader<UniParcEntryLight> {

    public UniParcLightRDDTupleReader(JobParameter jobParameter, boolean shouldRepartition) {
        super(jobParameter, shouldRepartition);
    }

    @Override
    protected Encoder<UniParcEntryLight> getEncoder() {
        Encoder<UniParcEntryLight> entryEncoder =
                (Encoder<UniParcEntryLight>) Encoders.kryo(UniParcEntryLight.class);
        return entryEncoder;
    }

    @Override
    protected MapFunction<Row, UniParcEntryLight> getConverter() {
        return new DatasetUniParcEntryLightConverter();
    }
}
