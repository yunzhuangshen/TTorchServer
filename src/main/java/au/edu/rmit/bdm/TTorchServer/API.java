package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.Torch;
import au.edu.rmit.bdm.TTorch.base.model.TrajEntry;
import au.edu.rmit.bdm.TTorch.queryEngine.Engine;
import au.edu.rmit.bdm.TTorch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.TTorch.queryEngine.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    String rangeQuery(String query){
        List<TrajEntry> entries = Converter.convertTrajectory(query);
        if (entries == null) return IdResponse.genFailed().toJSON();
        QueryResult ret = engine.findInRange(new SearchWindow(entries.get(0), entries.get(1)));
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    String pathQuery(String query){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed().toJSON();
        QueryResult ret = engine.findOnPath(queryPath);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    String pathQueryByStreetName(String query){
        QueryResult ret = engine.findOnPath(query);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    String strictPathQuery(String query){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed().toJSON();
        QueryResult ret = engine.findOnStrictPath(queryPath);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    String strictPathQueryByStreetName(String query){
        QueryResult ret = engine.findOnPath(query);
        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    
    String similarityQuery(String query, int k, String index, String measure, String epsilon){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed().toJSON();
        Map<String, String> props = new HashMap<>();
        props.put("simFunc", measure);
        props.put("index", index);
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
}
