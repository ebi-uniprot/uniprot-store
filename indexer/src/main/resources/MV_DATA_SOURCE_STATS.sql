CREATE MATERIALIZED VIEW MV_DATA_SOURCE_STATS AS

-- Cross Ref Begins

SELECT 'Cross Ref'                          AS data_type,
       COALESCE(reviewed.id, unreviewed.id) AS id,
       reviewed_protein_count,
       unreviewed_protein_count,
       NULL                                 AS referenced_proteome_count,
       NULL                                 AS proteome_count
FROM (
         SELECT dn.abbreviation              AS id,
                count(DISTINCT db.accession) AS reviewed_protein_count
         FROM sptr.dbentry db
                  JOIN sptr.dbentry_2_database d2d ON
             db.dbentry_id = d2d.dbentry_id
                  JOIN sptr.database_name dn ON
             dn.database_id = d2d.database_id
         WHERE db.entry_type IN 0
           AND db.deleted = 'N'
           AND db.merge_status <> 'R'
         GROUP BY dn.abbreviation) reviewed
         FULL JOIN (
    SELECT dn.abbreviation              AS id,
           count(DISTINCT db.accession) AS unreviewed_protein_count
    FROM sptr.dbentry db
             JOIN sptr.dbentry_2_database d2d ON
        db.dbentry_id = d2d.dbentry_id
             JOIN sptr.database_name dn ON
        dn.database_id = d2d.database_id
    WHERE db.entry_type IN 1
      AND db.deleted = 'N'
      AND db.merge_status <> 'R'
    GROUP BY dn.abbreviation) unreviewed ON
    reviewed.id = unreviewed.id
     -- Cross Ref Ends

UNION ALL

-- Subcellular Location Begins
SELECT 'Subcellular Location'               AS data_type,
       COALESCE(r.identifier, u.identifier) AS id,
       r.reviewedProteinCount               AS reviewed_protein_count,
       u.unreviewedProteinCount             AS unreviewed_protein_count,
       NULL                                 AS referenced_proteome_count,
       NULL                                 AS proteome_count
FROM (
         SELECT text        identifier,
                count(1) AS reviewedProteinCount
         FROM (WITH temp AS (
             SELECT db.accession,
                    css.text
             FROM dbentry db,
                  comment_block cb,
                  comment_structure cs,
                  comment_substructure css
             WHERE db.dbentry_id = cb.dbentry_id
               AND cb.comment_block_id = cs.comment_block_id
               AND cs.COMMENT_STRUCTURE_ID = css.COMMENT_STRUCTURE_ID
               AND db.entry_type = 0
               AND cb.COMMENT_TOPICS_ID = 14
               AND cs.cc_structure_type_id = 4
               AND db.deleted = 'N'
               AND cb.text IS NULL)
               SELECT DISTINCT t.accession,
                               trim(regexp_substr(t.text, '[^,]+', 1, levels.column_value)) AS text
               FROM temp t,
                    TABLE (CAST(MULTISET(
                                SELECT LEVEL
                                FROM dual
                                CONNECT BY LEVEL <= LENGTH(regexp_replace(t.text, '[^,]+')) + 1) AS sys.OdciNumberList)) levels)
         GROUP BY text) r
         FULL JOIN (
    SELECT text        identifier,
           count(1) AS unreviewedProteinCount
    FROM (WITH temp AS (
        SELECT accession,
               SUBSTR(actual_text, str_start, str_end - str_start + 1) text
        FROM (
                 SELECT db.accession,
                        cb.text        actual_text,
                        CASE
                            WHEN INSTR(cb.text, ':') >= (
                                CASE
                                    WHEN INSTR(cb.text, 'Note') > 0 THEN INSTR(cb.text, 'Note')
                                    ELSE LENGTH(cb.text)
                                    END) THEN 0
                            ELSE INSTR(cb.text, ':')
                            END + 1 AS str_start,
                        CASE
                            WHEN INSTR(cb.text, 'Note') > 0 THEN INSTR(cb.text, 'Note') - 1
                            ELSE LENGTH(cb.text)
                            END     AS str_end
                 FROM dbentry db,
                      comment_block cb
                 WHERE db.dbentry_id = cb.dbentry_id
                   AND db.entry_type = 1
                   AND cb.COMMENT_TOPICS_ID = 14
                   AND db.deleted = 'N'
                   AND cb.text IS NOT NULL
                   AND merge_status <> 'R'))
          SELECT DISTINCT t.accession,
                          trim(regexp_substr(t.text, '[^.;,]+', 1, levels.column_value)) AS text
          FROM temp t,
               TABLE (CAST(MULTISET(
                           SELECT LEVEL
                           FROM dual
                           CONNECT BY LEVEL <= LENGTH(regexp_replace(t.text, '[^.;,]+')) + 1) AS sys.OdciNumberList)) levels)
    GROUP BY text) u ON
    r.identifier = u.identifier
WHERE COALESCE(r.identifier, u.identifier) IS NOT NULL

      -- Subcellular Location Ends

UNION ALL

-- Taxonomy Begins
SELECT 'Taxonomy'                   AS data_type,
       TO_CHAR(COALESCE(r.TAX_ID, u.TAX_ID)) AS id,
       r.reviewedProteinCount       AS reviewed_protein_count,
       u.unreviewedProteinCount     AS unreviewed_protein_count,
       pr.referenceProteomeCount    AS referenced_proteome_count,
       pc.proteomeCount             AS proteome_count
FROM (
         SELECT tax_id,
                count(1) AS reviewedProteinCount
         FROM SPTR.dbentry
         WHERE entry_type = 0
           AND deleted = 'N'
           AND merge_status <> 'R'
         GROUP BY tax_id) r
         FULL JOIN (
    SELECT tax_id,
           count(1) AS unreviewedProteinCount
    FROM SPTR.dbentry
    WHERE entry_type = 1
      AND deleted = 'N'
      AND merge_status <> 'R'
    GROUP BY tax_id) u ON
    r.TAX_ID = u.TAX_ID
         LEFT JOIN (
    SELECT proteome_Taxid,
           count(*) AS referenceProteomeCount
    FROM SPTR.proteome
    WHERE publish = 1
      AND IS_REFERENCE = 1
    GROUP BY proteome_Taxid) pr ON
    pr.proteome_Taxid = u.TAX_ID
         LEFT JOIN (
    SELECT proteome_Taxid,
           count(*) AS proteomeCount
    FROM SPTR.proteome
    WHERE publish = 1
      AND ((IS_REDUNDANT = 0
        OR COVERABLE_BY_REDUNDANCY = 0)
        AND IS_EXCLUDED = 0)
    GROUP BY proteome_Taxid) pc ON
    pc.proteome_Taxid = u.TAX_ID
     -- Taxonomy Ends

UNION ALL

-- Disease Begins
SELECT 'Disease'          AS data_type,
       DISEASE_IDENTIFIER AS id,
       COUNT(ACCESSION)   AS reviewed_protein_count,
       NULL               AS unreviewed_protein_count,
       NULL               AS referenced_proteome_count,
       NULL               AS proteome_count
FROM (
         SELECT DISTINCT db.ACCESSION,
                         db.ENTRY_TYPE,
                         TRIM(SUBSTR(css.TEXT, 0, INSTR(css.TEXT, ' ('))) DISEASE_IDENTIFIER
         FROM SPTR.DBENTRY db
                  JOIN SPTR.COMMENT_BLOCK cb ON
             db.DBENTRY_ID = cb.DBENTRY_ID
                  JOIN SPTR.CV_COMMENT_TOPICS ct ON
             ct.COMMENT_TOPICS_ID = cb.COMMENT_TOPICS_ID
                  JOIN SPTR.COMMENT_STRUCTURE cs ON
             cb.COMMENT_BLOCK_ID = cs.COMMENT_BLOCK_ID
                  JOIN SPTR.CV_CC_STRUCTURE_TYPE cst ON
             cs.CC_STRUCTURE_TYPE_ID = cst.CC_STRUCTURE_TYPE_ID
                  JOIN SPTR.COMMENT_SUBSTRUCTURE css ON
             cs.COMMENT_STRUCTURE_ID = css.COMMENT_STRUCTURE_ID
         WHERE ct.TOPIC = 'DISEASE'
           AND cst."TYPE" = 'DISEASE'
           AND db.ENTRY_TYPE = 0
           AND db.DELETED = 'N'
           AND db.MERGE_STATUS <> 'R')
GROUP BY DISEASE_IDENTIFIER
         -- Disease Ends

UNION ALL

-- Keyword Begins
SELECT 'Keyword'                          AS data_type,
       COALESCE(r.accession, u.accession) AS id,
       r.reviewedProteinCount             AS reviewed_protein_count,
       u.unreviewedProteinCount           AS unreviewed_protein_count,
       NULL                               AS referenced_proteome_count,
       NULL                               AS proteome_count
FROM (
         SELECT kw.accession,
                count(1) AS unreviewedProteinCount
         FROM sptr.dbentry db,
              sptr.dbentry_2_keyword dk,
              sptr.keyword kw
         WHERE dk.KEYWORD_ID = kw.KEYWORD_ID
           AND db.dbentry_id = dk.dbentry_id
           AND db.merge_status <> 'R'
           AND db.deleted = 'N'
           AND db.entry_type = 1
         GROUP BY kw.ACCESSION) u
         FULL JOIN (
    SELECT kw.accession,
           count(1) AS reviewedProteinCount
    FROM sptr.dbentry db,
         sptr.dbentry_2_keyword dk,
         sptr.keyword kw
    WHERE dk.KEYWORD_ID = kw.KEYWORD_ID
      AND db.dbentry_id = dk.dbentry_id
      AND db.merge_status <> 'R'
      AND db.deleted = 'N'
      AND db.entry_type = 0
    GROUP BY kw.accession) r ON
    u.accession = r.accession
UNION ALL
SELECT 'Keyword'                          AS data_type,
       COALESCE(r.accession, u.accession) AS id,
       r.reviewedProteinCount             AS reviewed_protein_count,
       u.unreviewedProteinCount           AS unreviewed_protein_count,
       NULL                               AS referenced_proteome_count,
       NULL                               AS proteome_count
FROM (
         SELECT category.accession,
                count(DISTINCT db.accession) AS reviewedProteinCount
         FROM sptr.dbentry db
                  INNER JOIN sptr.dbentry_2_keyword d2k ON
             db.dbentry_id = d2k.DBENTRY_ID
                  INNER JOIN (
             SELECT DISTINCT keyword_id,
                             CASE
                                 UPPER(SUBSTR(HIERARCHY, 0, INSTR(HIERARCHY, ':') - 1))
                                 WHEN 'CELLULAR COMPONENT' THEN 'KW-9998'
                                 WHEN 'DEVELOPMENTAL STAGE' THEN 'KW-9996'
                                 WHEN 'LIGAND' THEN 'KW-9993'
                                 WHEN 'TECHNICAL TERM' THEN 'KW-9990'
                                 WHEN 'CODING SEQUENCE DIVERSITY' THEN 'KW-9997'
                                 WHEN 'BIOLOGICAL PROCESS' THEN 'KW-9999'
                                 WHEN 'PTM' THEN 'KW-9991'
                                 WHEN 'MOLECULAR FUNCTION' THEN 'KW-9992'
                                 WHEN 'DISEASE' THEN 'KW-9995'
                                 WHEN 'DOMAIN' THEN 'KW-9994'
                                 END accession
             FROM sptr.KEYWORD_HIERARCHY) category ON
             d2k.KEYWORD_ID = category.keyword_id
         WHERE db.entry_type = 0
           AND db.MERGE_STATUS <> 'R'
           AND db.deleted = 'N'
         GROUP BY category.accession) r
         FULL JOIN (
    SELECT category.accession,
           count(DISTINCT db.accession) AS unreviewedProteinCount
    FROM sptr.dbentry db
             INNER JOIN sptr.dbentry_2_keyword d2k ON
        db.dbentry_id = d2k.DBENTRY_ID
             INNER JOIN (
        SELECT DISTINCT keyword_id,
                        CASE
                            UPPER(SUBSTR(HIERARCHY, 0, INSTR(HIERARCHY, ':') - 1))
                            WHEN 'CELLULAR COMPONENT' THEN 'KW-9998'
                            WHEN 'DEVELOPMENTAL STAGE' THEN 'KW-9996'
                            WHEN 'LIGAND' THEN 'KW-9993'
                            WHEN 'TECHNICAL TERM' THEN 'KW-9990'
                            WHEN 'CODING SEQUENCE DIVERSITY' THEN 'KW-9997'
                            WHEN 'BIOLOGICAL PROCESS' THEN 'KW-9999'
                            WHEN 'PTM' THEN 'KW-9991'
                            WHEN 'MOLECULAR FUNCTION' THEN 'KW-9992'
                            WHEN 'DISEASE' THEN 'KW-9995'
                            WHEN 'DOMAIN' THEN 'KW-9994'
                            END accession
        FROM sptr.KEYWORD_HIERARCHY) category ON
        d2k.KEYWORD_ID = category.keyword_id
    WHERE db.entry_type = 1
      AND db.MERGE_STATUS <> 'R'
      AND db.deleted = 'N'
    GROUP BY category.accession) u ON
    u.accession = r.accession
     -- Keyword Ends

UNION ALL

-- Literature Begins
SELECT 'Literature'                         AS data_type,
       COALESCE(r.primary_id, u.primary_id) AS id,
       r.reviewedProteinCount               AS reviewed_protein_count,
       u.unreviewedProteinCount             AS unreviewed_protein_count,
       NULL                                 AS referenced_proteome_count,
       NULL                                 AS proteome_count
FROM (
         SELECT p2d.primary_id,
                count(*) AS unreviewedProteinCount
         FROM sptr.publication_2_database p2d
                  INNER JOIN sptr.dbentry_2_publication d2p ON
             d2p.publication_id = p2d.publication_id
                  INNER JOIN sptr.dbentry db ON
             db.dbentry_id = d2p.dbentry_id
         WHERE p2d.database_id = 'U'
           AND db.merge_status <> 'R'
           AND db.deleted = 'N'
           AND db.entry_type = 1
         GROUP BY p2d.primary_id) u
         FULL JOIN (
    SELECT p2d.primary_id,
           count(*) AS reviewedProteinCount
    FROM sptr.publication_2_database p2d
             INNER JOIN sptr.dbentry_2_publication d2p ON
        d2p.publication_id = p2d.publication_id
             INNER JOIN sptr.dbentry db ON
        db.dbentry_id = d2p.dbentry_id
    WHERE p2d.database_id = 'U'
      AND db.merge_status <> 'R'
      AND db.deleted = 'N'
      AND db.entry_type = 0
    GROUP BY p2d.primary_id) r ON
    r.primary_id = u.primary_id
     -- Literature Ends

UNION ALL

-- UniRule Begins
SELECT 'UniRule'                            AS data_type,
       COALESCE(reviewed.id, unreviewed.id) AS id,
       reviewed.reviewed_protein_count      AS reviewed_protein_count,
       unreviewed.unreviewed_protein_count  AS unreviewed_protein_count,
       NULL                                 AS referenced_proteome_count,
       NULL                                 AS proteome_count
FROM (
         SELECT aar.source_id                 AS id,
                count(DISTINCT aar.accession) AS unreviewed_protein_count
         FROM dbentry db
                  JOIN CV_ENTRY_TYPE cet ON
             db.ENTRY_TYPE = cet.ENTRY_TYPE_ID
                  JOIN aa_active_rules aar ON
             db.accession = aar.accession
         WHERE db.deleted = 'N'
           AND db.merge_status <> 'R'
         GROUP BY aar.source_id) unreviewed
         FULL JOIN (
    SELECT SUBSTR(eadb."ATTRIBUTE", INSTR(eadb."ATTRIBUTE", ':') + 1) AS id,
           count(DISTINCT db.DBENTRY_ID)                              AS reviewed_protein_count
    FROM DBENTRY db
             JOIN EVID_2_ALL_DBENTRY_V eadb ON
        db.DBENTRY_ID = eadb.DBENTRY_ID
             JOIN CV_EVIDENCE_TYPE cet ON
        eadb.EVIDENCE_TYPE_ID = cet.EVIDENCE_TYPE_ID
    WHERE db.ENTRY_TYPE = 0
      AND db.DELETED = 'N'
      AND db.merge_status <> 'R'
      AND cet."TYPE" = 'C_HAMAP-Rule'
    GROUP BY eadb."ATTRIBUTE") reviewed ON
    reviewed.id = unreviewed.id;
-- UniRule Ends