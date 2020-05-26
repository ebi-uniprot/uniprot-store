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

create table taxonomy.V_PUBLIC_STRAIN
(
     TAX_ID     INTEGER,
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

create table TAXONOMY.V_PUBLIC_MERGED
(
     OLD_TAX_ID     INTEGER,
     NEW_TAX_ID     INTEGER
);

create table TAXONOMY.V_PUBLIC_DELETED
(
     TAX_ID     INTEGER
);

CREATE SCHEMA SPTR;

create table SPTR.MV_DATA_SOURCE_STATS
(
DATA_TYPE VARCHAR2(64) NOT NULL,
ID VARCHAR2(128) NOT NULL,
REVIEWED_PROTEIN_COUNT BIGINT,
UNREVIEWED_PROTEIN_COUNT BIGINT,
REFERENCED_PROTEOME_COUNT BIGINT,
PROTEOME_COUNT BIGINT
);

create table SPTR.MV_DATA_SOURCE_STATS
(
DATA_TYPE VARCHAR2(64) NOT NULL,
ID VARCHAR2(128) NOT NULL,
REVIEWED_PROTEIN_COUNT BIGINT,
UNREVIEWED_PROTEIN_COUNT BIGINT,
REFERENCED_PROTEOME_COUNT BIGINT,
PROTEOME_COUNT BIGINT
);




