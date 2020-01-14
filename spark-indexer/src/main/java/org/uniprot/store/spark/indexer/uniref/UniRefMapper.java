package org.uniprot.store.spark.indexer.uniref;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * This class is responsible to Map MappedUniRef into UniProtDocument based on UniRefType
 *
 * @author lgonzales
 * @since 2019-10-22
 */
public class UniRefMapper
        implements Function<Tuple2<UniProtDocument, Optional<MappedUniRef>>, UniProtDocument> {

    private static final long serialVersionUID = -7600564687228805786L;

    /**
     * @param tuple of {key=UniProtDocument, value=MappedUniRef}
     * @return UniProtDocument with added MappedUniRef information.
     */
    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple)
            throws Exception {
        UniProtDocument document = tuple._1;
        if (tuple._2.isPresent()) {
            MappedUniRef uniRef = (MappedUniRef) tuple._2.get();
            switch (uniRef.getUniRefType()) {
                case UniRef50:
                    document.unirefCluster50 = uniRef.getClusterID();
                    break;
                case UniRef90:
                    document.unirefCluster90 = uniRef.getClusterID();
                    break;
                case UniRef100:
                    document.unirefCluster100 = uniRef.getClusterID();
                    break;
            }
            if (Utils.notNull(uniRef.getUniparcUPI())) {
                document.uniparc = uniRef.getUniparcUPI();
            }
        }
        return document;
    }
}
