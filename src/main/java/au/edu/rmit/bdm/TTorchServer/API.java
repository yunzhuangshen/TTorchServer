package au.edu.rmit.bdm.TTorchServer;

import java.util.ArrayList;
import java.util.List;

class API {
    private Engine engine;

    API(){
        engine = Engine.getBuilder().build();
    }


    String rangeQuery(String _lat, String _lng, String _range){
        double  lat = Double.parseDouble(_lat),
                lng = Double.parseDouble(_lng),
                range = Double.parseDouble(_range);

//        return engine.findInRange(new SearchWindow(new Coordinate(lat, lng), range)).getMapVFormat();
        return "hello world";
    }

    String pathQuery(String query){
//        List<? extends TrajEntry> queryPath = convert(query);
//        return engine.findOnPath(queryPath).getMapVFormat();
        return "hello world";
    }

    String strictPathQuery(String query){
//        List<? extends TrajEntry> queryPath = convert(query);
//        return engine.findOnStrictPath(queryPath).getMapVFormat();
        return "hello world";
    }

    String similarityQuery(String query, int k){
//        List<? extends TrajEntry> queryPath = convert(query);
//        return engine.findTopK(queryPath, k).getMapVFormat();
        return "hello world";
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
