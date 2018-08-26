package au.edu.rmit.bdm.TTorchServer;


import au.edu.rmit.bdm.Torch.queryEngine.model.TimeInterval;
import au.edu.rmit.bdm.Torch.queryEngine.model.TorchDate;
import com.google.gson.Gson;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.servlet.SparkApplication;
import java.util.*;
import java.util.stream.Collectors;

import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class App implements SparkApplication {

    Logger logger = LoggerFactory.getLogger(App.class);;
    API api;
    Gson gson;

    @Override
    public void init() {
        BasicConfigurator.resetConfiguration();
        PropertyConfigurator.configure(App.class.getResourceAsStream("/log4j.properties"));
        Spark.exception(Exception.class, (exception, request, response) -> exception.printStackTrace());
        staticFiles.location("/public");
        logger.info("start to setup server");
        initMainPage();
        initAPI();
        gson = new Gson();
        logger.info("server setup complete");
    }

    private void initMainPage() {
        get("/", (req,res) -> {
            res.redirect("index.html");
            return null;
        });
    }

    /**
     * Get a string of JSON format<p>
     *
     * key-value map:
     *
     * - key: formatCorrect
     * value: Boolean value indicates if the string passed in could be modeled as a trajectory
     *
     * - key: queryResult
     * value:
     *    - key: queryType
     *    @see au.edu.rmit.bdm.Torch.base.Torch.QueryType for possible queryType types as value
     *
     *    - key: mappingSucceed:
     *    value: Boolean value indicates if the process of converting raw trajectory to map-matched trajectory succeeds.
     *
     *    - key: raw
     *    Query in mapV format.
     *    Or null if the queryType is of type rangeQuery
     *
     *    - key: mapped
     *    mapmatched queryType in mapV format.
     *    Or null if the queryType is of type rangeQuery
     *
     *    - key: retSize
     *    value: integer indicates number of qualified trajectories found
     *
     *    - key: ret
     *    value: array of qualified trajectories in mapV format
     */
    private void initAPI() {

        api = new API();

        get("/API/MULTI", (req, res) -> {

            logger.info("receive request /API/MULTI : {}", req.queryParams("items"));
            String queryArr = req.queryParams("items");
            String city = req.queryParams("city");

            api.setTimeFrame(model(req.queryParams("start"), req.queryParams("end")));
            QueryJson[] queries = gson.fromJson(queryArr, QueryJson[].class);

            List<IdResponse> l = new LinkedList<>();
            for (QueryJson q: queries){
                switch (q.type){
                    case "PQ":
                        l.add(api.pathQuery(q.query, city));
                        break;
                    case "SPQ":
                        l.add(api.strictPathQuery(q.query, city));
                        break;
                    case "NPQ":
                        l.add(api.pathQueryByStreetName(q.query, city));
                        break;
                    case "SNPQ":
                        l.add(api.strictPathQueryByStreetName(q.query, city));
                        break;
                    case "RQ":
                        l.add(api.rangeQuery(q.query, city));
                        break;
                }
            }

            //find union
            Set<Integer> set1 = new HashSet<>();
            Set<Integer> set2 = new HashSet<>();
            boolean first = true;
            for (IdResponse idRes : l){
                if (idRes.retObj.retSize == 0) continue;

                if (first) {
                    first = false;
                    set1.addAll(Arrays.stream(idRes.retObj.ids).boxed().collect(Collectors.toSet()));
                }else{
                    for (Integer id : idRes.retObj.ids){
                        if (set2.contains(id)){
                            set1.add(id);
                        }
                    }
                }

                set2 = set1;
                set1 = new HashSet<>();
                idRes.retObj.ids = null;
            }

            int k = (int)Math.ceil(set2.size() / 200);
            Integer[] ids = new Integer[set2.size()];
            System.arraycopy(set2.toArray(), 0, ids, 0, set2.size());

            String cluster = api.clustering(set2, k > 10 ? 10 : k, city);
            String arr = gson.toJson(l);
            String temp = gson.toJson(new MultiJSON(gson.toJson(ids), arr, cluster));
            logger.info("ret: {}" + temp);
            return temp;
        });


        get("/API/TKQ", (req, res) -> {
            logger.info("receive request /API/TKQ : {}", req.queryParams("query"));
            api.setTimeFrame(model(req.queryParams("start"), req.queryParams("end")));

            return api.similarityQuery(
                    req.queryParams("query"),
                    Integer.parseInt(req.queryParams("k")),
                    req.queryParams("measure"),
                    req.queryParams("epsilon"),
                    req.queryParams("city"));
        });

        get("/API/ID", (req, res) -> {
            logger.info("receive request /API/ID : {}", req.queryParams("idSet"));
            return api.resolveIDs(req.queryParams("idSet"), req.queryParams("city"));
        });
    }

    private TimeInterval model(String start, String end){
        start = start.replace('T', ' ')+":00";
        end = end.replace('T', ' ')+":00";
        logger.debug("time constraint: [{}] -- [{}]", start, end);
        return new TimeInterval(new TorchDate().setAll(start), new TorchDate().setAll(end));
    }
}
