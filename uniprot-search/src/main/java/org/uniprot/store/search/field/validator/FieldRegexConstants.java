package org.uniprot.store.search.field.validator;

/**
 * This class defines regular expressions as constants, for reference by other classes if needed.
 * Note that this provides static, compile time access to some regular expressions for fields.
 * However, the defacto source is {@link
 * org.uniprot.store.config.searchfield.common.SearchFieldConfig}, via the {@link
 * org.uniprot.store.config.searchfield.common.SearchFieldConfig#isSearchFieldValueValid(String,
 * String)} (String, String)} method.
 *
 * @author lgonzales
 */
public class FieldRegexConstants {
    public static final String UNIPROTKB_ACCESSION_REGEX =
            "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])(-[0-9]+)?";
    public static final String UNIPROTKB_ACCESSION_OR_ID =
            "([OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z]([0-9][A-Z][A-Z0-9]{2}){1,2}[0-9])((-[0-9]{1,3})|(\\.[0-9]{1,3})|(_[A-Z0-9]{2,5}))?|[A-Z0-9]{2,5}_[A-Z0-9]{2,5}";

    public static final String PROTEOME_ID_REGEX = "UP[0-9]{9}";
    public static final String UNIPARC_UPI_REGEX = "UPI[\\w]{10}";
    public static final String UNIREF_CLUSTER_ID_REGEX =
            "(UniRef100|UniRef90|UniRef50)_\\w+(-[0-9]+)?";
    public static final String SEQUENCE_REGEX = "^[A-Z]+$|^NULL$";
    public static final String COMMA_SEPARATED_REGEX = "\\s*,\\s*";
    public static final String CLEAN_QUERY_REGEX = "(?:^\\()|(?:\\)$)";
    public static final String GO_ID_REGEX = "^GO:\\d{7}$";
    public static final String EC_ID_REGEX =
            "^([1-7])\\.("
                    + "((\\d{1,2})\\.(\\d{1,2})\\.(\\d{1,3}|n\\d{1,2}|-))|"
                    + "((\\d{1,2})\\.(\\d{1,2}|-)\\.-)|"
                    + "((\\d{1,2}|-)\\.-\\.-)"
                    + ")$";
    public static final String KEYWORD_ID_REGEX = "^KW-\\d{4}$";
    public static final String TAXONOMY_ID_REGEX = "^\\d+$";

    private FieldRegexConstants() {}
}
