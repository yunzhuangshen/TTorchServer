package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.Torch;
import au.edu.rmit.bdm.TTorch.base.model.TrajEntry;
import au.edu.rmit.bdm.TTorch.queryEngine.Engine;
import au.edu.rmit.bdm.TTorch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.TTorch.queryEngine.model.TimeInterval;
import au.edu.rmit.bdm.TTorch.queryEngine.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


class API {
    private Engine engine;
    private Logger logger;

    API(){
        logger = LoggerFactory.getLogger(API.class);
        String basePath = App.class.getResource("/").getPath();
        engine = Engine.getBuilder().resolveResult(false).baseURI(basePath).build();
    }

    IdResponse rangeQuery(String query){
        List<TrajEntry> entries = Converter.convertTrajectory(query);
        if (entries == null) return IdResponse.genFailed();
        QueryResult ret = engine.findInRange(new SearchWindow(entries.get(0), entries.get(1)));
        logger.info("number of results found: {}", ret.retSize);
        logger.info("idArray: {}", Arrays.toString(ret.idArray));
        return IdResponse.genSuccessful(ret);
    }

    IdResponse pathQuery(String query){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed();
        QueryResult ret = engine.findOnPath(queryPath);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse pathQueryByStreetName(String query){
        QueryResult ret = engine.findOnPath(query);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse strictPathQuery(String query){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed();
        QueryResult ret = engine.findOnStrictPath(queryPath);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse strictPathQueryByStreetName(String query){
        QueryResult ret = engine.findOnPath(query);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    
    String similarityQuery(String query, int k, String measure, String epsilon){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed().toJSON();
        Map<String, String> props = new HashMap<>();
        props.put("simFunc", measure);
        props.put("epsilon", epsilon);
        engine.update(Torch.QueryType.TopK, props);
        QueryResult ret = engine.findTopK(queryPath, k);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    

    String resolveIDs(String idSet) {
        return engine.resolve(jsonArr2intArr(idSet)).getRetMapVformat();
    }

    private int[] jsonArr2intArr(String idSet){
        String[] _ids = idSet.substring(1, idSet.length() - 1).split(",");
        int[] ids = new int[_ids.length];
        for (int i = 0; i < _ids.length; i++)
            ids[i] = Integer.valueOf(_ids[i]);
        return ids;
    }

    public void setTimeFrame(TimeInterval queryTimeInterval) {
        engine.setTimeInterval(queryTimeInterval, true);
    }
}
