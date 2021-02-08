package org.uniprot.store.indexer.taxonomy;

public class TaxonomySQLConstants {

    private TaxonomySQLConstants() {}

    public static final String SELECT_TAXONOMY_NODE_SQL =
            "SELECT tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,"
                    + "ncbi_scientific,ncbi_common,sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum"
                    + " FROM taxonomy.v_public_node";

    public static final String COUNT_PROTEINS_SQL =
            "SELECT ID as TAX_ID, REVIEWED_PROTEIN_COUNT as reviewedProteinCount, UNREVIEWED_PROTEIN_COUNT as unreviewedProteinCount, "
                    + "REFERENCED_PROTEOME_COUNT as referenceProteomeCount, PROTEOME_COUNT as proteomeCount "
                    + "FROM SPTR.MV_DATA_SOURCE_STATS WHERE DATA_TYPE = 'Taxonomy'";

    public static final String SELECT_TAXONOMY_STRAINS_SQL =
            "SELECT STRAIN_ID, NAME, NAME_CLASS"
                    + " FROM TAXONOMY.v_public_strain "
                    + " WHERE tax_id = ?";

    public static final String SELECT_TAXONOMY_LINEAGE_SQL =
            "SELECT"
                    + "   SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id,"
                    + "   SYS_CONNECT_BY_PATH(SPTR_SCIENTIFIC, '|') AS lineage_name,"
                    + "   SYS_CONNECT_BY_PATH(COALESCE(SPTR_COMMON, NCBI_COMMON, ' '), '|') AS lineage_common,"
                    + "   SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank,"
                    + "   SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_hidden"
                    + " FROM taxonomy.V_PUBLIC_NODE"
                    + " WHERE TAX_ID = 1"
                    + " START WITH TAX_ID = ?"
                    + " CONNECT BY PRIOR PARENT_ID = TAX_ID";

    public static final String SELECT_TAXONOMY_HOSTS_SQL =
            "SELECT n.tax_id,ncbi_scientific,ncbi_common,"
                    + " sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code"
                    + " FROM taxonomy.v_public_node n INNER JOIN TAXONOMY.V_PUBLIC_HOST h on n.tax_id = h.host_id"
                    + " WHERE h.tax_id = ?";

    public static final String SELECT_TAXONOMY_OTHER_NAMES_SQL =
            "SELECT nm.NAME"
                    + " FROM taxonomy.V_PUBLIC_NAME nm INNER JOIN TAXONOMY.V_PUBLIC_NODE nd  on nm.TAX_ID = nd.TAX_ID"
                    + " WHERE nm.PRIORITY > 0 AND"
                    + " (UPPER(nm.NAME) <> UPPER(nd.SPTR_COMMON) OR nd.SPTR_COMMON is null) AND"
                    + " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SCIENTIFIC) OR nd.SPTR_SCIENTIFIC is null) AND"
                    + " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SYNONYM) OR nd.SPTR_SYNONYM is null) AND"
                    + " (UPPER(nm.NAME) <> UPPER(nd.TAX_CODE) OR nd.TAX_CODE is null)"
                    + " AND nm.TAX_ID = ?";

    public static final String SELECT_TAXONOMY_LINKS_SQL =
            "SELECT URI " + " FROM TAXONOMY.V_PUBLIC_URI" + " WHERE tax_id = ?";

    public static final String SELECT_TAXONOMY_MERGED_SQL =
            "select old_tax_id,new_tax_id from taxonomy.v_public_merged";

    public static final String SELECT_TAXONOMY_DELETED_SQL =
            "select tax_id from taxonomy.v_public_deleted";
}
