package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.model.Coordinate;
import au.edu.rmit.bdm.TTorch.base.model.TrajEntry;
import au.edu.rmit.bdm.TTorch.queryEngine.Engine;
import au.edu.rmit.bdm.TTorch.queryEngine.model.SearchWindow;

import java.util.ArrayList;
import java.util.List;

class API {
    private Engine engine;

    API(){
        String basePath = App.class.getResource("/").getPath();
        engine = Engine.getBuilder().baseURI(basePath).build();
    }

    //todo
    String rangeQuery(String query){

        List<TrajEntry> entries = convert(query);

        return engine.findInRange(new SearchWindow(entries.get(0), entries.get(1)).getRetMapVformat();
    }

    String pathQuery(String query){
        List<? extends TrajEntry> queryPath = convert(query);
        return engine.findOnPath(queryPath).getRetMapVformat();
    }

    String strictPathQuery(String query){
        List<? extends TrajEntry> queryPath = convert(query);
        return engine.findOnStrictPath(queryPath).getRetMapVformat();
    }

    String similarityQuery(String query, int k){
        List<? extends TrajEntry> queryPath = convert(query);
        return engine.findTopK(queryPath, k).getRetMapVformat();
    }

    private List<TrajEntry> convert(String coords){
        int len = coords.length();
        coords = coords.substring(2, len-2); // remove leading "[[" and trailing "]]"
        String[] tuples = coords.split("],\\[");


        List<TrajEntry> entries = new ArrayList<>(tuples.length);
        String[] temp;
        double lat;
        double lng;
        for (String tuple : tuples){
            temp = tuple.split(",");
            lat = Double.valueOf(temp[0]);
            lng = Double.valueOf(temp[1]);
            entries.add(new Coordinate(lat, lng));
        }

        return entries;
    }
}
