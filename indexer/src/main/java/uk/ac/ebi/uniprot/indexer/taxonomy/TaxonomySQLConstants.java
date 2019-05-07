package uk.ac.ebi.uniprot.indexer.taxonomy;

public class TaxonomySQLConstants {

    public static final String SELECT_TAXONOMY_NODE_SQL = "select tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id," +
            "ncbi_scientific,ncbi_common,sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum" +
            " from taxonomy.v_public_node";// where tax_id < 11000"; //TODO: remove this filter

    public static final String COUNT_PROTEINS_SQL = "select COALESCE(r.TAX_ID,u.TAX_ID) as TAX_ID, r.reviewedProteinCount, u.unreviewedProteinCount, p.proteomeCount" +
            " from (select tax_id, count(1) as reviewedProteinCount" +
            "               from SPTR.dbentry" +
            "               where entry_type = 0 and deleted ='N' and merge_status<>'R'" +
//            "                 AND TAX_ID < 11000" + //TODO: remove this filter
            "               group by tax_id) r" +
            "    full join (select tax_id, count(1) as unreviewedProteinCount" +
            "                 from SPTR.dbentry" +
            "                 where entry_type = 1 and deleted ='N' and merge_status<>'R'" +
//            "                   AND TAX_ID < 11000" + //TODO: remove this filter
            "                 group by tax_id) u on r.TAX_ID = u.TAX_ID" +
            "   left join (select proteome_Taxid, count(1) as proteomeCount " +
            "                 from SPTR.proteome " +
            "                 where publish=1" +
//            "                       and proteome_Taxid < 11000" + //TODO: remove proteome_Taxid filter
            "                 group by proteome_Taxid) p on p.proteome_Taxid = u.TAX_ID";

    public static final String SELECT_TAXONOMY_STRAINS_SQL = "SELECT STRAIN_ID, NAME, NAME_CLASS" +
            " from TAXONOMY.v_public_strain " +
            " where tax_id = ?";

    public static final String SELECT_TAXONOMY_LINEAGE_SQL = "select" +
            "   SYS_CONNECT_BY_PATH(TAX_ID, '|') AS lineage_id," +
            "   SYS_CONNECT_BY_PATH(SPTR_SCIENTIFIC, '|') AS lineage_name," +
            "   SYS_CONNECT_BY_PATH(RANK, '|') AS lineage_rank," +
            "   SYS_CONNECT_BY_PATH(HIDDEN, '|') AS lineage_hidden" +
            " from taxonomy.V_PUBLIC_NODE" +
            " WHERE TAX_ID = 1" +
            " START WITH TAX_ID = ?" +
            " CONNECT BY PRIOR PARENT_ID = TAX_ID";

    public static final String SELECT_TAXONOMY_HOSTS_SQL = "select n.tax_id,ncbi_scientific,ncbi_common," +
            " sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code" +
            " from taxonomy.v_public_node n inner join TAXONOMY.V_PUBLIC_HOST h on n.tax_id = h.host_id" +
            " where h.tax_id = ?";

    public static final String SELECT_TAXONOMY_OTHER_NAMES_SQL = "select nm.NAME" +
            " from taxonomy.V_PUBLIC_NAME nm inner join TAXONOMY.V_PUBLIC_NODE nd  on nm.TAX_ID = nd.TAX_ID" +
            " where nm.PRIORITY > 0 AND" +
            " (UPPER(nm.NAME) <> UPPER(nd.SPTR_COMMON) OR nd.SPTR_COMMON is null) AND" +
            " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SCIENTIFIC) OR nd.SPTR_SCIENTIFIC is null) AND" +
            " (UPPER(nm.NAME) <> UPPER(nd.SPTR_SYNONYM) OR nd.SPTR_SYNONYM is null) AND" +
            " (UPPER(nm.NAME) <> UPPER(nd.TAX_CODE) OR nd.TAX_CODE is null)" +
            " AND nm.TAX_ID = ?";

    public static final String SELECT_TAXONOMY_LINKS_SQL = "select URI " +
            " from TAXONOMY.V_PUBLIC_URI" +
            " where tax_id = ?";
}
