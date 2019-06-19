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

INSERT INTO SPTR.PROTEOME(PROTEOME_ID, PROTEOME_TAXID, PUBLISH, IS_COMPLETE, IS_REFERENCE) VALUES (1, 5, 1, 0, 1);
INSERT INTO SPTR.PROTEOME(PROTEOME_ID, PROTEOME_TAXID, PUBLISH, IS_COMPLETE, IS_REFERENCE) VALUES (2, 5, 1, 1, 1);
INSERT INTO SPTR.PROTEOME(PROTEOME_ID, PROTEOME_TAXID, PUBLISH, IS_COMPLETE, IS_REFERENCE) VALUES (2, 4, 1, 0, 1);

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

-- start --- data for cross ref test
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(23940, 'Q96X30', 4, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(102528, 'Q8NJ52', 4, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(112082, 'Q8NIN9', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(131221, 'Q9Y749', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(214025, 'Q96VP4', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(249222, 'Q8TFM8', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(253164, 'Q9Y750', 5, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(349869, 'O00089', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(363683, 'O60023', 5, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
(DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES(419848, 'Q9Y8B8', 5, 0, 'N', 'N');
INSERT INTO SPTR.DBENTRY
  (DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES (419849, 'Q9Y8B9', 1, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
  (DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES (419847, 'Q9Y8B7', 1, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
  (DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES (419846, 'Q9Y8B6', 1, 1, 'N', 'N');
INSERT INTO SPTR.DBENTRY
  (DBENTRY_ID, ACCESSION, TAX_ID, ENTRY_TYPE, DELETED, MERGE_STATUS)
VALUES (419845, 'Q9Y8B5', 1, 1, 'N', 'N');

INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(23940, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(23940, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(102528, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(112082, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(131221, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(167129, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(214025, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(214025, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(249222, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(249222, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(253164, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(253164, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(292296, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(349869, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(349869, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(363683, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(419848, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(419848, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(425369, 'ALLER');
INSERT INTO SPTR.DBENTRY_2_DATABASE
(DBENTRY_ID, DATABASE_ID)
VALUES(425369, 'ALLER');

INSERT INTO SPTR.DATABASE_NAME
(ABBREVIATION, DATABASE_ID)
VALUES('Allergome', 'ALLER');
-- end --- data for cross ref test

-- start --- keyword extra data
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (1, 'KW-0001');
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (2, 'KW-0002');
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (3, 'KW-0003');
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (4, 'KW-0004');
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (5, 'KW-0005');
INSERT INTO SPTR.KEYWORD (KEYWORD_ID, ACCESSION)
VALUES (6, 'KW-0006');

INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (1, 23940);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (1, 102528);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (1, 112082);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (2, 131221);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (2, 214025);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (2, 249222);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (2, 419846);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (3, 253164);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (3, 349869);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (3, 363683);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (4, 419848);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (4, 419849);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (5, 37532507);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (5, 419847);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (6, 37232139);
INSERT INTO SPTR.DBENTRY_2_KEYWORD (KEYWORD_ID, DBENTRY_ID)
VALUES (6, 419847);

INSERT INTO SPTR.KEYWORD_HIERARCHY (KEYWORD_ID, HIERARCHY)
VALUES (1, 'CELLULAR COMPONENT: value');
INSERT INTO SPTR.KEYWORD_HIERARCHY (KEYWORD_ID, HIERARCHY)
VALUES (2, 'LIGAND: value');
INSERT INTO SPTR.KEYWORD_HIERARCHY (KEYWORD_ID, HIERARCHY)
VALUES (3, 'MOLECULAR FUNCTION: value');
INSERT INTO SPTR.KEYWORD_HIERARCHY (KEYWORD_ID, HIERARCHY)
VALUES (4, 'DISEASE: value');
-- end --- keyword extra data
