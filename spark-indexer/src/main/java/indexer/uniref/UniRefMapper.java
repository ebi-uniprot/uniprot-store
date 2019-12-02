package indexer.uniref;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-10-22
 */
public class UniRefMapper
        implements Function<Tuple2<UniProtDocument, Optional<MappedUniRef>>, UniProtDocument> {

    private static final long serialVersionUID = -7600564687228805786L;

    @Override
    public UniProtDocument call(Tuple2<UniProtDocument, Optional<MappedUniRef>> tuple)
            throws Exception {
        UniProtDocument document = tuple._1;
        if (tuple._2.isPresent()) {
            MappedUniRef uniRef = (MappedUniRef) tuple._2.get();
            switch (uniRef.getUniRefType()) {
                case UniRef50:
                    document.unirefCluster50.add(uniRef.getClusterID());
                    document.unirefCluster50.addAll(uniRef.getMemberAccessions());
                    document.unirefSize50 = uniRef.getMemberSize();
                    break;
                case UniRef90:
                    document.unirefCluster90.add(uniRef.getClusterID());
                    document.unirefCluster90.addAll(uniRef.getMemberAccessions());
                    document.unirefSize90 = uniRef.getMemberSize();
                    break;
                case UniRef100:
                    document.unirefCluster100.add(uniRef.getClusterID());
                    document.unirefCluster100.addAll(uniRef.getMemberAccessions());
                    document.unirefSize100 = uniRef.getMemberSize();
                    break;
            }
            if (uniRef.getUniRefMember() != null
                    && uniRef.getUniRefMember().getUniParcId() != null) {
                document.uniparc = uniRef.getUniRefMember().getUniParcId().getValue();
            }
        }
        return document;
    }
}
