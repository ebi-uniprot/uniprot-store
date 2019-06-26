package uk.ac.ebi.uniprot.search.expr;

import java.util.List;

/**
 * Created 26/06/19
 *
 * @author Edd
 */
public interface BoostExpressions {
    List<BoostExpression> getBoosts();
}
