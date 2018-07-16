package au.edu.rmit.bdm.T_Torch.base;

import au.edu.rmit.bdm.T_Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.T_Torch.queryEngine.model.LightEdge;

import java.util.List;

public interface TopKQueryIndex extends Index{
    <T extends TrajEntry> List<String> findTopK(int k, List<T> pointQuery, List<LightEdge> edgeQuery);
    boolean useEdge();
}
