package au.edu.rmit.bdm.T_Torch.base;

import au.edu.rmit.bdm.T_Torch.queryEngine.model.LightEdge;

import java.util.List;


public interface PathQueryIndex extends Index{

    List<String> findByPath(List<LightEdge> path);
    List<String> findByStrictPath(List<LightEdge> path);
}
