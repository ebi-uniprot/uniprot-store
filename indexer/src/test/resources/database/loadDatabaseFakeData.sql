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
'rank_4','gc_id_4','mgc_id_4','ncbi_scientific_4','ncbi_common_4','Sptr_Scientific_4','Sptr_Common_4','sptr_synonym_4',
'sptr_code_4','Tax_Code_4','sptr_ff_4','superregnum_4');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (5,4,1,'internal_5',
'rank_5','gc_id_5','mgc_id_5','ncbi_scientific_5','ncbi_common_5','Sptr_Scientific_5','Sptr_Common_5','sptr_synonym_5',
'sptr_code_5','Tax_Code_5','sptr_ff_5','superregnum_5');

insert into taxonomy.tax_public (tax_id) values (1);
insert into taxonomy.tax_public (tax_id) values (2);
insert into taxonomy.tax_public (tax_id) values (3);
insert into taxonomy.tax_public (tax_id) values (4);
insert into taxonomy.tax_public (tax_id) values (5);

insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (1,4);
insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (2,5);

insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (1,1,'strain 1','synonym');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (1,2,'strain 2','scientific name');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (2,3,'strain 3','synonym');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (2,4,'strain 4','scientific name');

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

insert into sptr.dbentry (TAX_ID,entry_type,deleted,merge_status) values (5,0,'N','A');
insert into sptr.dbentry (TAX_ID,entry_type,deleted,merge_status) values (5,1,'N','A');
insert into sptr.dbentry (TAX_ID,entry_type,deleted,merge_status) values (5,0,'N','A');
insert into sptr.dbentry (TAX_ID,entry_type,deleted,merge_status) values (5,1,'N','A');