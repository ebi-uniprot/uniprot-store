package indexer.uniprot;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import java.io.Serializable;
import java.util.HashMap;

/**
 * @author lgonzales
 * @since 2019-09-30
 */
public class UniprotDocumentConverter {

    public static JavaPairRDD<String, UniProtDocument> convert(JavaPairRDD<String, UniProtEntry> uniProtEntryRDD) {
        return (JavaPairRDD<String, UniProtDocument>) uniProtEntryRDD.mapValues(new UniProtEntryToSolrDocumentConverter());
    }


    private static class UniProtEntryToSolrDocumentConverter implements Serializable, Function<UniProtEntry, UniProtDocument> {

        private static final long serialVersionUID = -6891371730036443245L;

        @Override
        public UniProtDocument call(UniProtEntry uniProtEntry) throws Exception {
            UniProtEntryConverter converter = new UniProtEntryConverter(null, null, null, null, null, new HashMap<>());
            return converter.convert(uniProtEntry);
        }
    }

}
