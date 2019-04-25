insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (1,null,0,'internal_1',
'rank_1','gc_id_1','mgc_id_1','ncbi_scientific_1','ncbi_common_1','Sptr_Scientific_1','Sptr_Common_1','sptr_synonym_1',
'sptr_code_1','Tax_Code_1','sptr_ff_1','superregnum_1');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (2,1,1,'internal_2',
'rank_2','gc_id_2','mgc_id_2','Ncbi_Scientific_2','Ncbi_Common_2',null,null,'sptr_synonym_2',
'sptr_code_2','Tax_Code_2','sptr_ff_2','superregnum_2');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (3,1,1,'internal_3',
'rank_3','gc_id_3','mgc_id_3','ncbi_scientific_3','ncbi_common_3','Sptr_Scientific_3','Sptr_Common_3','sptr_synonym_3',
'sptr_code_3','Tax_Code_3','sptr_ff_3','superregnum_3');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (4,1,1,'internal_4',
'rank_4','gc_id_4','mgc_id_4','ncbi_scientific_4','ncbi_common_4','Sptr_Scientific_4','Sptr_Common_4','sptr_synonym_4',
'sptr_code_4','Tax_Code_4','sptr_ff_4','superregnum_4');
insert into taxonomy.v_public_node (tax_id,parent_id,hidden,internal,rank,gc_id,mgc_id,ncbi_scientific,ncbi_common,
sptr_scientific,sptr_common,sptr_synonym,sptr_code,tax_code,sptr_ff,superregnum) values (5,1,1,'internal_5',
'rank_5','gc_id_5','mgc_id_5','ncbi_scientific_5','ncbi_common_5','Sptr_Scientific_5','Sptr_Common_5','sptr_synonym_5',
'sptr_code_5','Tax_Code_5','sptr_ff_5','superregnum_5');

insert into taxonomy.tax_public (tax_id) values (1);
insert into taxonomy.tax_public (tax_id) values (2);
insert into taxonomy.tax_public (tax_id) values (3);
insert into taxonomy.tax_public (tax_id) values (4);
insert into taxonomy.tax_public (tax_id) values (5);

insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (1,1);
insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (2,1);
insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (3,2);
insert into taxonomy.SPTR_STRAIN (STRAIN_ID,tax_id) values (4,3);

insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (1,1,'strain 1','synonym');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (2,2,'strain 2','scientific name');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (3,3,'strain 3','synonym');
insert into taxonomy.SPTR_STRAIN_NAME (STRAIN_ID,NAME_ID, NAME, NAME_CLASS) values (4,4,'strain 4','scientific name');

insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (1,4);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (1,5);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (2,4);
insert into taxonomy.V_PUBLIC_HOST (tax_id, host_id) values (2,5);

insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (1,'uri 1');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (1,'uri 2');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (2,'uri 3');
insert into TAXONOMY.V_PUBLIC_URI (tax_id, URI) values (2,'uri 4');

insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (1,-1,'not valid');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (1,1,'other taxonomy name data');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (1,2,'sptr_scientific_1');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (1,3,'sptr_common_1');
insert into TAXONOMY.V_PUBLIC_NAME (TAX_ID,PRIORITY,NAME) values (1,4,'sptr_code_1');

INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(16612935520, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(22414243661, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(22414231782, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(25648814569, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(24655257961, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(24181371378, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(25650646225, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(22413171730, 'Rheumatoid arthritis (RA) [MIM:180300]: An inflammatory disease with autoimmune features and a complex genetic component. It primarily affects the joints and is characterized by inflammatory changes in the synovial membranes and articular structures, widespread fibrinoid degeneration of the collagen fibers in mesenchymal tissues, and by atrophy and rarefaction of bony structures.');

INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(16612935515, 8, 16612935520);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(22414243658, 8, 22414243661);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(24181371377, 8, 24181371378);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(22414231781, 8, 22414231782);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(25648814567, 8, 25648814569);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(25650646224, 8, 25650646225);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(22413171729, 8, 22413171730);
INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(24655257959, 8, 24655257961);

INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(113648107, 30, 16612935515);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(385861484, 30, 22414243658);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(37232139, 30, 25648814567);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(2995441, 30, 24655257959);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(38238153, 30, 22414231781);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(37532507, 30, 25650646224);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(36359821, 30, 22413171729);
INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(3563849, 30, 24181371377);

INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(113648107, 'Q9H015', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(3563849, 'Q29980', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(36359821, 'Q13568', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(385861484, 'Q96P31', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(2995441, 'Q9UBC1', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(38238153, 'Q14765', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(37232139, 'Q9UM07', 9606, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(37532507, 'Q9Y2R2', 9606, 0, 'N', 'N');


-- Another entry
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(26423496975, 'Zimmermann-Laband syndrome 1 (ZLS1) [MIM:135500]: A disorder characterized by gingival fibromatosis, dysplastic or absent nails, finger abnormalities, hepatosplenomegaly, and abnormalities of the cartilage of the nose and/or ears.');

INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(26423496973, 8, 26423496975);

INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(36426324, 30, 26423496973);

INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(36426324, 'O95259', 9606, 0, 'N', 'N');


-- Another entry
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(20768780386, 'Zimmermann-Laband syndrome 2 (ZLS2) [MIM:616455]: A disorder characterized by gingival fibromatosis, dysplastic or absent nails, finger abnormalities, hepatosplenomegaly, and abnormalities of the cartilage of the nose and/or ears.');

INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(20768780385, 8, 20768780386);

INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(38700307, 30, 20768780385);

INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(38700307, 'P21281', 9606, 0, 'N', 'N');


-- Another Entry
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(24350786595, 'Zinc deficiency, transient neonatal (TNZD) [MIM:608118]: A disorder occurring in breast-fed infants as a consequence of low milk zinc concentration in their nursing mothers, which cannot be corrected by maternal zinc supplementation. A large amount of zinc, an essential trace mineral, is required for normal growth particularly in infants, and breast milk normally contains adequate zinc to meet the requirement for infants up to 4 to 6 months of age. Zinc deficiency can lead to dermatitis, alopecia, decreased growth, and impaired immune function. The disorder shows autosomal dominant inheritance with incomplete penetrance.');

INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(24350786592, 8, 24350786595);

INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(221555878, 30, 24350786592);

INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(221555878, 'Q9BRI3', 9606, 0, 'N', 'N');


-- Another entry
INSERT INTO SPTR.COMMENT_SUBSTRUCTURE
(COMMENT_STRUCTURE_ID, TEXT)
VALUES(24659050116, 'ZTTK syndrome (ZTTKS) [MIM:617140]: An autosomal dominant syndrome characterized by intellectual disability, developmental delay, malformations of the cerebral cortex, epilepsy, vision problems, musculo-skeletal abnormalities, and congenital malformations.');

INSERT INTO SPTR.COMMENT_STRUCTURE
(COMMENT_BLOCK_ID, CC_STRUCTURE_TYPE_ID, COMMENT_STRUCTURE_ID)
VALUES(24659050115, 8, 24659050116);

INSERT INTO SPTR.COMMENT_BLOCK
(DBENTRY_ID, COMMENT_TOPICS_ID, COMMENT_BLOCK_ID)
VALUES(38177856, 30, 24659050115);

INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(38177856, 'P18583', 9606, 0, 'N', 'N');

-- Constant entries
INSERT INTO SPTR.CV_COMMENT_TOPICS
(COMMENT_TOPICS_ID, TOPIC)
VALUES(30, 'DISEASE');

INSERT INTO SPTR.CV_CC_STRUCTURE_TYPE
(CC_STRUCTURE_TYPE_ID, "TYPE")
VALUES(8, 'DISEASE');




