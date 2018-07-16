package au.edu.rmit.bdm.T_Torch.queryEngine.query;

import au.edu.rmit.bdm.T_Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.T_Torch.base.model.Trajectory;
import au.edu.rmit.bdm.T_Torch.mapMatching.algorithm.Mapper;

import java.util.List;

abstract class QueryImpl implements Query{
    protected List<TrajEntry> raw;
    protected Trajectory<TrajEntry> mapped;
    protected Mapper mapper;
    protected TrajectoryResolver resolver;

    protected QueryImpl(Mapper mapper, TrajectoryResolver resolver){
        this.mapper = mapper;
        this.resolver = resolver;
    }

    @Override
    public boolean prepare(List<? extends TrajEntry> raw) {
        this.raw = (List<TrajEntry>)raw;
        Trajectory<TrajEntry> t = new Trajectory<>();
        t.addAll(raw);

        try {
            mapped = (Trajectory<TrajEntry>)(Object)mapper.match(t);
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
