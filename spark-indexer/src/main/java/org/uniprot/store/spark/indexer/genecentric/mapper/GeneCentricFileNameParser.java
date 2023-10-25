package org.uniprot.store.spark.indexer.genecentric.mapper;

import java.io.Serializable;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public interface GeneCentricFileNameParser extends Serializable {
    default String parseProteomeId(FileSplit fileSplit) {
        String fileName = fileSplit.getPath().getName();
        return fileName.substring(0, fileName.indexOf("_"));
    }
}
