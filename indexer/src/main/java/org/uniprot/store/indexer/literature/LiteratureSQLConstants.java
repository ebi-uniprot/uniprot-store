package org.uniprot.store.indexer.literature;

/** @author lgonzales */
public class LiteratureSQLConstants {

    public static final String LITERATURE_STATISTICS_SQL =
            "SELECT COALESCE(r.primary_id,u.primary_id) as pubmed_id, r.reviewedProteinCount, u.unreviewedProteinCount "
                    + "FROM ( "
                    + "    SELECT p2d.primary_id, count(*) as unreviewedProteinCount FROM sptr.publication_2_database p2d "
                    + "      INNER JOIN sptr.dbentry_2_publication d2p ON d2p.publication_id = p2d.publication_id "
                    + "      INNER JOIN sptr.dbentry db ON db.dbentry_id = d2p.dbentry_id "
                    + "    WHERE p2d.database_id = 'U' "
                    + "      AND db.merge_status <> 'R' "
                    + "      AND db.deleted = 'N' "
                    + "      AND db.entry_type = 0 "
                    + "    GROUP BY p2d.primary_id) u "
                    + "FULL JOIN ( "
                    + "    SELECT p2d.primary_id, count(*) as reviewedProteinCount FROM sptr.publication_2_database p2d "
                    + "      INNER JOIN sptr.dbentry_2_publication d2p ON d2p.publication_id = p2d.publication_id "
                    + "      INNER JOIN sptr.dbentry db ON db.dbentry_id = d2p.dbentry_id "
                    + "    WHERE p2d.database_id = 'U' "
                    + "      AND db.merge_status <> 'R' "
                    + "      AND db.deleted = 'N' "
                    + "      AND db.entry_type = 1 "
                    + "    GROUP BY p2d.primary_id) r "
                    + "ON r.primary_id = u.primary_id";
}
