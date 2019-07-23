package uk.ac.ebi.uniprot.indexer.subcell;

/**
 * @author lgonzales
 * @since 2019-07-12
 */
public class SubcellularLocationSQLConstants {

    public static final String SUBCELLULAR_LOCATION_STATISTICS_QUERY =
            "SELECT COALESCE(r.identifier,u.identifier) as identifier, r.reviewedProteinCount, u.unreviewedProteinCount " +
                    "  FROM " +
                    "    (SELECT text identifier, count(1) as reviewedProteinCount " +
                    "       FROM ( " +
                    "        WITH temp as ( " +
                    "           SELECT db.accession, css.text " +
                    "           FROM dbentry db, comment_block cb, comment_structure cs, comment_substructure css " +
                    "           WHERE db.dbentry_id = cb.dbentry_id " +
                    "             AND cb.comment_block_id =cs.comment_block_id " +
                    "             AND cs.COMMENT_STRUCTURE_ID =css.COMMENT_STRUCTURE_ID " +
                    "             AND db.entry_type =0 " +
                    "             AND cb.COMMENT_TOPICS_ID=14 " +
                    "             AND cs.cc_structure_type_id = 4 " +
                    "             AND db.deleted='N' " +
                    "             AND cb.text is null " +
                    "          ) " +
                    "          SELECT distinct " +
                    "           t.accession, trim(regexp_substr(t.text, '[^,]+', 1, levels.column_value)) as text " +
                    "          FROM temp t, " +
                    "              table(cast(multiset( SELECT level FROM dual connect by level <= length (regexp_replace(t.text, '[^,]+')) + 1) as sys.OdciNumberList)) levels " +
                    "       ) " +
                    "    GROUP BY text) r " +
                    "FULL JOIN " +
                    "  (SELECT text identifier, count(1) as unreviewedProteinCount " +
                    "    FROM ( " +
                    "       WITH temp as ( " +
                    "         SELECT accession, SUBSTR(actual_text, str_start, str_end-str_start+1) text " +
                    "         FROM ( " +
                    "                 SELECT db.accession, cb.text actual_text, " +
                    "                       case when INSTR(cb.text, ':') >= (case when INSTR(cb.text, 'Note') > 0 then INSTR(cb.text, 'Note') ELSE LENGTH(cb.text) END) then 0 else INSTR(cb.text, ':') end + 1 AS str_start, " +
                    "                       case when INSTR(cb.text, 'Note') > 0 then INSTR(cb.text, 'Note')-1 ELSE LENGTH(cb.text) END AS str_end " +
                    "                FROM dbentry db, comment_block cb " +
                    "                WHERE db.dbentry_id = cb.dbentry_id " +
                    "                  AND db.entry_type =1 " +
                    "                  AND cb.COMMENT_TOPICS_ID=14 " +
                    "                  AND db.deleted='N' " +
                    "                  AND cb.text is not null " +
                    "                  AND merge_status<>'R' " +
                    "              ) " +
                    "         ) " +
                    "          SELECT distinct " +
                    "           t.accession, trim(regexp_substr(t.text, '[^.;,]+', 1, levels.column_value)) as text " +
                    "         FROM temp t, " +
                    "              table(cast(multiset(SELECT level FROM dual connect by level <= length (regexp_replace(t.text, '[^.;,]+')) + 1) as sys.OdciNumberList)) levels " +
                    "     ) " +
                    "GROUP BY text) u ON r.identifier = u.identifier " +
                    "WHERE COALESCE(r.identifier,u.identifier) is not null";

}
