package uk.ac.ebi.uniprot.search.expr;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Created 26/06/19
 *
 * @author Edd
 */
public class UniProtKBBoostExpressions implements BoostExpressions {
    @Override
    public List<BoostExpression> getBoosts() {
        // TODO: 26/06/19 check what happens when user query is reviewed:false. Does this boost override false?
        return singletonList(BoostExpression.builder().expression("reviewed:true").boost(8.0F).build());
    }
}
