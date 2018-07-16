package au.edu.rmit.bdm.T_Torch.base;

import au.edu.rmit.bdm.T_Torch.queryEngine.model.SearchWindow;

import java.util.List;

public interface WindowQueryIndex extends Index{
    List<String> findInRange(SearchWindow window);
}
