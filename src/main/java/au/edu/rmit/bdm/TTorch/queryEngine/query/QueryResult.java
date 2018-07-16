package au.edu.rmit.bdm.TTorch.queryEngine.query;

import au.edu.rmit.bdm.TTorch.base.model.TrajEntry;
import au.edu.rmit.bdm.TTorch.base.model.Trajectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryResult {
    private static final Logger logger = LoggerFactory.getLogger(QueryResult.class);

    public final boolean succeed;
    private List<TrajEntry> rawQuery;
    private List<TrajEntry> mappedQuery;
    private List<Trajectory<TrajEntry>> ret;

    public QueryResult(boolean succeed){
        this.succeed = succeed;
    }

    QueryResult(List<Trajectory<TrajEntry>> ret, List<TrajEntry> rawQuery, List<TrajEntry> mappedQuery){
        this.succeed = true;
        this.ret = ret;
        this.rawQuery = rawQuery;
        this.mappedQuery = mappedQuery;
    }

    public String getMapVFormat(){
        if (rawQuery == null)  // this indicates the query type is window query, which do not have second and third params
            return TrajJsonFormater.toJSON(ret);

        return TrajJsonFormater.toJSON(ret, rawQuery, mappedQuery);
    }

    public List<Trajectory<TrajEntry>> getResultTrajectory(){
        return ret;
    }
}
