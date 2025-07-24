package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

/**
 * Extracts individual <entry>...</entry> blocks from an XML line stream. Intended for use with
 * Spark flatMap to convert a block of XML lines into entry-level strings.
 */
public class GoogleUniProtXMLEntryExtractor implements FlatMapFunction<Iterator<String>, String> {

    @Override
    public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
        List<String> entries = new ArrayList<>();
        StringBuilder currentEntry = null;
        boolean insideEntry = false;
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            // Start a new entry if an opening <entry> tag is found
            if (line.contains("<entry")) {
                insideEntry = true;
                currentEntry = new StringBuilder();
            }
            // If currently inside an entry, accumulate the line
            if (insideEntry) {
                currentEntry.append(line).append("\n");
            }
            // End of entry detected; add to list and reset state
            if (line.contains("</entry>")) {
                if (insideEntry && currentEntry != null) {
                    entries.add(currentEntry.toString());
                }
                insideEntry = false;
                currentEntry = null;
            }
        }

        return entries.iterator();
    }
}
