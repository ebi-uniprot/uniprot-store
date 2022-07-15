package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.scorer.uniprotkb.UniProtEntryScored;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
public class UniProtKBAnnotationScoreMapper
        implements Serializable, Function<UniProtKBEntry, UniProtKBEntry> {
    private static final long serialVersionUID = -4999658812951835083L;

    @Override
    public UniProtKBEntry call(UniProtKBEntry uniProtKBEntry) throws Exception {
        UniProtEntryScored entryScored = new UniProtEntryScored(uniProtKBEntry);

        UniProtKBEntryBuilder builder = UniProtKBEntryBuilder.from(uniProtKBEntry);
        int score = SparkUtils.scaleAnnotationScore(entryScored.score());
        builder.annotationScore(score);
        return builder.build();
    }
}
