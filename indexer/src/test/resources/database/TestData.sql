insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (1,null,1,'internal_1',
'rank_1','gc_id_1','mgc_id_1','ncbi_scientific_1','ncbi_common_1','Sptr_Scientific_1','Sptr_Common_1','sptr_synonym_1',
'sptr_code_1','Tax_Code_1','sptr_ff_1','superregnum_1');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (2,1,1,'internal_2',
'rank_2','gc_id_2','mgc_id_2','Ncbi_Scientific_2','Ncbi_Common_2',null,null,'sptr_synonym_2',
'sptr_code_2','Tax_Code_2','sptr_ff_2','superregnum_2');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (3,2,1,'internal_3',
'rank_3','gc_id_3','mgc_id_3','ncbi_scientific_3','ncbi_common_3','Sptr_Scientific_3','Sptr_Common_3','sptr_synonym_3',
'sptr_code_3','Tax_Code_3','sptr_ff_3','superregnum_3');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (4,3,1,'internal_4',
'KINGDOM','gc_id_4','mgc_id_4','ncbi_scientific_4','ncbi_common_4','Sptr_Scientific_4','Sptr_Common_4','sptr_synonym_4',
'sptr_code_4','Tax_Code_4','sptr_ff_4','superregnum_4');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (5,4,1,'internal_5',
'FAMILY','gc_id_5','mgc_id_5','ncbi_scientific_5','ncbi_common_5','Sptr_Scientific_5','Sptr_Common_5','sptr_synonym_5',
'sptr_code_5','Tax_Code_5','sptr_ff_5','superregnum_5');

insert into taxonomy.tax_public (tax_id) values (1);
insert into taxonomy.tax_public (tax_id) values (2);
insert into taxonomy.tax_public (tax_id) values (3);
insert into taxonomy.tax_public (tax_id) values (4);
insert into taxonomy.tax_public (tax_id) values (5);

insert into taxonomy.V_PUBLIC_STRAIN (TAX_ID, STRAIN_ID, NAME, NAME_CLASS) values (5,1,'strain 1,syn 1 ','synonym');
insert into taxonomy.V_PUBLIC_STRAIN (TAX_ID, STRAIN_ID, NAME, NAME_CLASS) values (5,1,'strain 1,syn 2','synonym');
insert into taxonomy.V_PUBLIC_STRAIN (TAX_ID, STRAIN_ID, NAME, NAME_CLASS) values (5,1,'strain 1','scientific name');
insert into taxonomy.V_PUBLIC_STRAIN (TAX_ID, STRAIN_ID, NAME, NAME_CLASS) values (5,2,'strain 2 syn 1','synonym');
insert into taxonomy.V_PUBLIC_STRAIN (TAX_ID, STRAIN_ID, NAME, NAME_CLASS) values (5,2,'strain 2','scientific name');

insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (4,4);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (4,5);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (5,4);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (5,5);

insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (5,'uri 1');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (5,'uri 2');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (4,'uri 3');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (4,'uri 4');

insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,-1,'not valid');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,1,'first name');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,2,'second name');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,3,'sptr_scientific_5');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,4,'sptr_common_5');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (5,5,'tax_Code_5');

insert into TAXONOMY.V_PUBLIC_MERGED(OLD_TAX_ID,NEW_TAX_ID) values (50,5);
insert into TAXONOMY.V_PUBLIC_MERGED(OLD_TAX_ID,NEW_TAX_ID) values (40,5);

insert into TAXONOMY.V_PUBLIC_DELETED(TAX_ID) values (500);
insert into TAXONOMY.V_PUBLIC_DELETED(TAX_ID) values (400);

-- mv unirule data begins --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
        ('UniRule','PIRNR018063',NULL,239,NULL,NULL);
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
        ('UniRule','PIRSR006661-1',NULL,5412,NULL,NULL);
-- mv unirule data ends --
-- start of taxonomy stats test data --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
                                       VALUES ('Taxonomy','5',6,2,2,1);
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
                                        VALUES ('Taxonomy','4',6,2,1,1);
-- end of taxonomy stats test data --

-- start subcellular location stats data starts --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Acidocalcisome lumen', 10, 20, NULL, NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Nucleolus', 5, 6, NULL, NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Nucleus lamina', 6, NULL, NULL, NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Nucleus matrix', 7, 8, NULL, NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Perinuclear region', 8, 9, NULL, NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE, ID, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT, PROTEOME_COUNT)
VALUES ('Subcellular Location', 'Nucleoplasm', 9, 10, NULL, NULL);
-- start subcellular location stats data ends --

-- start literature stats data --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
VALUES ('Literature','11203701',1,1,NULL,NULL);

-- end literature stats data --

-- start keyword stats data --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
VALUES('Keyword','KW-9993',3,1,NULL,NULL);
-- end keyword stats data --

-- start disease stats data --
INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
VALUES('Disease','Rheumatoid arthritis',8,NULL,NULL,NULL);
-- end disease stats data --

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,
                                       REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT)
VALUES ('Cross Ref','Allergome',1283,3167,NULL,NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
('Cross Ref','BindingDB',5261,537,NULL,NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
('Cross Ref','Bgee',56889,502642,NULL,NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
('Cross Ref','ArachnoServer',1154,199,NULL,NULL);

INSERT INTO SPTR.MV_DATA_SOURCE_STATS (DATA_TYPE,ID,REVIEWED_PROTEIN_COUNT,UNREVIEWED_PROTEIN_COUNT,REFERENCED_PROTEOME_COUNT,PROTEOME_COUNT) VALUES
('Cross Ref','Araport',15907,32533,NULL,NULL);


