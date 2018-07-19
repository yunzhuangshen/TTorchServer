package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.Torch;
import au.edu.rmit.bdm.TTorch.base.model.Coordinate;
import au.edu.rmit.bdm.TTorch.base.model.TrajEntry;
import au.edu.rmit.bdm.TTorch.queryEngine.Engine;
import au.edu.rmit.bdm.TTorch.queryEngine.model.SearchWindow;
import au.edu.rmit.bdm.TTorch.queryEngine.query.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class API {
    private Engine engine;
    private static Logger logger = LoggerFactory.getLogger(API.class);

    API(){
        String basePath = App.class.getResource("/").getPath();
        engine = Engine.getBuilder().baseURI(basePath).build();
    }

    String rangeQuery(String query){

        List<TrajEntry> entries = convert(query);
        return entries == null ? Response.genFailed().toJSON() :
                Response.genSuccessful(engine.findInRange(new SearchWindow(entries.get(0), entries.get(1)))).toJSON();
    }

    String pathQuery(String query){
        List<? extends TrajEntry> queryPath = convert(query);
        return queryPath == null ? Response.genFailed().toJSON() :
                Response.genSuccessful(engine.findOnPath(queryPath)).toJSON();
    }

    String strictPathQuery(String query){
        List<? extends TrajEntry> queryPath = convert(query);
        return queryPath == null ? Response.genFailed().toJSON() :
                Response.genSuccessful(engine.findOnStrictPath(queryPath)).toJSON();
    }

    String similarityQuery(String query, int k){
        List<? extends TrajEntry> queryPath = convert(query);
        return queryPath == null ? Response.genFailed().toJSON() :
                Response.genSuccessful(engine.findTopK(queryPath, k)).toJSON();
    }

    /**
     * Try to model incoming string to a list of trajEntry.
     * If the string is in wrong format and can not be modeled,
     * null will be returned.
     */
    private List<TrajEntry> convert(String coords){
        try {
            int len = coords.length();
            coords = coords.substring(2, len - 2); // remove leading "[[" and trailing "]]"
            String[] tuples = coords.split("],\\[");


            List<TrajEntry> entries = new ArrayList<>(tuples.length);
            String[] temp;
            double lat;
            double lng;
            for (String tuple : tuples) {
                temp = tuple.split(",");
                lat = Double.valueOf(temp[0]);
                lng = Double.valueOf(temp[1]);
                entries.add(new Coordinate(lat, lng));
            }
            return entries;

        }catch (Exception e){
            logger.warn(e.getMessage());
        }
        return null;
    }
}
