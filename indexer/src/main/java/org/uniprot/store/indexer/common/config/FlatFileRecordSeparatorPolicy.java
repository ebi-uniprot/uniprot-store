package org.uniprot.store.indexer.common.config;

import org.springframework.batch.item.file.separator.SuffixRecordSeparatorPolicy;

/**
 * @author lgonzales
 */
public class FlatFileRecordSeparatorPolicy extends SuffixRecordSeparatorPolicy {

    private String suffix;

    @Override
    public void setSuffix(String suffix) {
        super.setSuffix(suffix);
        this.suffix = suffix;
    }

    @Override
    public String preProcess(String line) {
        return line + "\n";
    }

    @Override
    public boolean isEndOfRecord(String line) {
        if (line == null) {
            return true;
        } else {
            return line.trim().endsWith(this.suffix);
        }
    }
}
