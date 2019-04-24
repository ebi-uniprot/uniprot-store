CREATE SCHEMA taxonomy;

CREATE TABLE taxonomy.v_public_node
(
     tax_id INTEGER,
     parent_id INTEGER,
     hidden INTEGER,
     internal VARCHAR(64),
     rank VARCHAR(64),
     gc_id VARCHAR(64),
     mgc_id VARCHAR(64),
     ncbi_scientific VARCHAR(64),
     ncbi_common VARCHAR(64),
     sptr_scientific VARCHAR(64),
     sptr_common VARCHAR(64),
     sptr_synonym VARCHAR(64),
     sptr_code VARCHAR(64),
     tax_code VARCHAR(64),
     sptr_ff VARCHAR(64),
     superregnum VARCHAR(64)
);

CREATE TABLE taxonomy.tax_public (
     tax_id INTEGER
);

create table taxonomy.SPTR_STRAIN
(
     STRAIN_ID  INTEGER,
     TAX_ID     INTEGER
);

create table taxonomy.SPTR_STRAIN_NAME
(
     NAME_ID    INTEGER,
     STRAIN_ID  INTEGER,
     NAME       VARCHAR2(64),
     NAME_CLASS VARCHAR2(64)
);

create table taxonomy.V_PUBLIC_HOST
(
     TAX_ID     INTEGER,
     HOST_ID    INTEGER
);

create table TAXONOMY.V_PUBLIC_URI
(
     TAX_ID     INTEGER,
     URI     VARCHAR2(64)
);

create table TAXONOMY.V_PUBLIC_NAME
(
     TAX_ID     INTEGER,
     PRIORITY   INTEGER,
     NAME       VARCHAR2(64)
);

CREATE SCHEMA sptr;

create table sptr.dbentry
(
     TAX_ID     INTEGER,
     entry_type     INTEGER,
     deleted VARCHAR2(64),
     merge_status VARCHAR2(64)
);