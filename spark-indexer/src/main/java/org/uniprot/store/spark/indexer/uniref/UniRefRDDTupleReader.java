package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryConverter;

/**
 * Responsible to Load JavaRDD{UniRefEntry} for a specific UniRefType
 *
 * @author lgonzales
 * @since 2019-10-16
 */
public class UniRefRDDTupleReader implements RDDReader<UniRefEntry> {

    private final JobParameter jobParameter;
    private final UniRefType uniRefType;
    private final boolean shouldRepartition;

    public UniRefRDDTupleReader(
            UniRefType uniRefType, JobParameter jobParameter, boolean shouldRepartition) {
        this.uniRefType = uniRefType;
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    public JavaRDD<UniRefEntry> load() {
        JavaRDD<Row> uniRefEntryDataset = loadRawXml(uniRefType, jobParameter).toJavaRDD();
        if (shouldRepartition) {
            uniRefEntryDataset =
                    uniRefEntryDataset.repartition(uniRefEntryDataset.getNumPartitions() * 7);
        }

        return uniRefEntryDataset.map(new DatasetUniRefEntryConverter(uniRefType));
    }
}
