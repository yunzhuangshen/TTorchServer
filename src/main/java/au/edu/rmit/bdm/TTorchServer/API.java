package au.edu.rmit.bdm.TTorchServer;


import au.edu.rmit.bdm.Torch.base.Torch;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import au.edu.rmit.bdm.Torch.clustering.kpaths.Process;
import au.edu.rmit.bdm.Torch.queryEngine.Engine;
import au.edu.rmit.bdm.Torch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.Torch.queryEngine.model.TimeInterval;
import au.edu.rmit.bdm.Torch.queryEngine.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

class API {
    private Engine beijingEngine;
    private Engine portoEngine;
    private Logger logger;

    API(){
        logger = LoggerFactory.getLogger(API.class);
        String basePath = App.class.getResource("/").getPath();

        beijingEngine = Engine.getBuilder().
                resolveResult(false).
                baseDir("Torch_Porto").
                URIprefix(basePath).
                build();

        portoEngine = Engine.getBuilder().
                resolveResult(false).
                baseDir("Torch_Beijing").
                URIprefix(basePath).
                build();

        try {
            //todo clustering code
            Process.init();
            logger.info("init process success!");
        } catch (IOException e) {
            logger.info("error in init process(clustering)");
        }
    }

    IdResponse rangeQuery(String query, String city){
        List<TrajEntry> entries = Converter.convertTrajectory(query);
        if (entries == null) return IdResponse.genFailed();
        QueryResult ret = null;

            if (city.equalsIgnoreCase(TorchServer.BEIJING))
                ret = beijingEngine.findInRange(new SearchWindow(entries.get(0), entries.get(1)));
            else if (city.equalsIgnoreCase(TorchServer.PORTO))
                ret = portoEngine.findInRange(new SearchWindow(entries.get(0), entries.get(1)));
            else{
                logger.error("The city from client is not recognized");
            }

        logger.info("number of results foubaseDir(\"Torch_Porto\").nd: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse pathQuery(String query, String city){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed();
        QueryResult ret = beijingEngine.findOnPath(queryPath);

        if (city.equalsIgnoreCase(TorchServer.BEIJING))
            ret = beijingEngine.findOnPath(queryPath);
        else if (city.equalsIgnoreCase(TorchServer.PORTO))
            ret = portoEngine.findOnPath(queryPath);
        else{
            logger.error("The city from client is not recognized");
        }

        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse pathQueryByStreetName(String query, String city){
        QueryResult ret = null;

        if (city.equalsIgnoreCase(TorchServer.BEIJING))
            beijingEngine.findOnPath(query);
        else if (city.equalsIgnoreCase(TorchServer.PORTO))
            portoEngine.findOnPath(query);
        else{
            logger.error("The city from client is not recognized");
        }

        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse strictPathQuery(String query, String city){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed();
        QueryResult ret = null;

        if (city.equalsIgnoreCase(TorchServer.BEIJING))
            ret = beijingEngine.findOnStrictPath(queryPath);
        else if (city.equalsIgnoreCase(TorchServer.PORTO))
            ret = portoEngine.findOnStrictPath(query);
        else{
            logger.error("The city from client is not recognized");
        }

        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    IdResponse strictPathQueryByStreetName(String query, String city){
        QueryResult ret = null;

        if (city.equalsIgnoreCase(TorchServer.BEIJING))
            ret = beijingEngine.findOnStrictPath(query);
        else if (city.equalsIgnoreCase(TorchServer.PORTO))
            ret = portoEngine.findOnStrictPath(query);
        else{
            logger.error("The city from client is not recognized");
        }

        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret);
    }

    
    String similarityQuery(String query, int k, String measure, String epsilon, String city){
        List<? extends TrajEntry> queryPath = Converter.convertTrajectory(query);
        if (queryPath == null) return IdResponse.genFailed().toJSON();
        Map<String, String> props = new HashMap<>();
        props.put("simFunc", measure);
        props.put("epsilon", epsilon);

        QueryResult ret = null;
        if (city.equalsIgnoreCase(TorchServer.BEIJING)) {
            beijingEngine.update(Torch.QueryType.TopK, props);
            ret = beijingEngine.findTopK(queryPath, k);
        }else if (city.equalsIgnoreCase(TorchServer.PORTO)) {
            portoEngine.update(Torch.QueryType.TopK, props);
            ret = portoEngine.findOnStrictPath(query);
        }else{
            logger.error("The city from client is not recognized");
        }

        logger.info("number of results found: {}", ret.retSize);
        return IdResponse.genSuccessful(ret).toJSON();
    }

    String clustering(Set<Integer> ids, int k, String city){

        try {
            int[] clusters = Process.clustering(ids, k);
            return resolveIDs(clusters, city);

        } catch (IOException e) {
            logger.info("error when clustering");
        }
        return "";
    }

    String resolveIDs(String idSet, String city) {
        return beijingEngine.resolve(jsonArr2intArr(idSet)).getRetMapVformat();
    }

    private String resolveIDs(int[] idSet, String city){

        return beijingEngine.resolve(idSet).getRetMapVformat();
    }

    private int[] jsonArr2intArr(String idSet){
        String[] _ids = idSet.substring(1, idSet.length() - 1).split(",");
        int[] ids = new int[_ids.length];
        for (int i = 0; i < _ids.length; i++)
            ids[i] = Integer.valueOf(_ids[i]);
        return ids;
    }

    public void setTimeFrame(TimeInterval queryTimeInterval) {
        beijingEngine.setTimeInterval(queryTimeInterval, true);
        portoEngine.setTimeInterval(queryTimeInterval, true);
    }
}
