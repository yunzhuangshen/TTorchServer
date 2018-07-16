package au.edu.rmit.bdm.T_Torch.queryEngine.query;

import au.edu.rmit.bdm.T_Torch.base.WindowQueryIndex;
import au.edu.rmit.bdm.T_Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.T_Torch.queryEngine.model.SearchWindow;

import java.util.List;

class WindowQuery extends QueryImpl {

    private WindowQueryIndex index;

    WindowQuery(WindowQueryIndex index, TrajectoryResolver resolver){
        super(null, resolver);
        this.index = index;
    }

    @Override
    public QueryResult execute(Object windowRange) {
        if (!(windowRange instanceof SearchWindow))
            throw new IllegalStateException(
                    "parameter passed to windowQuery should be of type SearchWindow, " +
                    "which indicates the range to search within");

        SearchWindow window = (SearchWindow) windowRange;
        List<String> trajIds = index.findInRange(window);
        return resolver.resolve(trajIds, null, null);
    }

    @Override
    public boolean prepare(List<? extends TrajEntry> raw) {
        return true;
    }
}
