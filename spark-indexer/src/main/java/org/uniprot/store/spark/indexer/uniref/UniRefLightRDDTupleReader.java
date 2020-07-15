package org.uniprot.store.spark.indexer.uniref;

import static org.uniprot.store.spark.indexer.uniref.UniRefXmlUtils.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.UniRefType;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.reader.RDDReader;
import org.uniprot.store.spark.indexer.uniref.converter.DatasetUniRefEntryLightConverter;

/**
 * Responsible for loading the JavaRDD {@link UniRefEntryLight} for a specific {@link UniRefType}
 *
 * <p>Created 08/07/2020
 *
 * @author Edd
 */
public class UniRefLightRDDTupleReader implements RDDReader<UniRefEntryLight> {

    private final JobParameter jobParameter;
    private final UniRefType uniRefType;
    private final boolean shouldRepartition;

    public UniRefLightRDDTupleReader(
            UniRefType uniRefType, JobParameter jobParameter, boolean shouldRepartition) {
        this.uniRefType = uniRefType;
        this.jobParameter = jobParameter;
        this.shouldRepartition = shouldRepartition;
    }

    public JavaRDD<UniRefEntryLight> load() {
        JavaRDD<Row> uniRefEntryDataset = loadRawXml(uniRefType, jobParameter).toJavaRDD();
        if (shouldRepartition) {
            uniRefEntryDataset =
                    uniRefEntryDataset.repartition(uniRefEntryDataset.getNumPartitions() * 7);
        }

        return uniRefEntryDataset.map(new DatasetUniRefEntryLightConverter(uniRefType));
    }


}
