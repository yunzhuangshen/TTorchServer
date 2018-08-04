package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.Torch;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.servlet.SparkApplication;

import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class App implements SparkApplication {
    Logger logger = LoggerFactory.getLogger(App.class);;

    private API api;

    public static void main(String[] args){
        "abc".split(".",-3);


    }

    @Override
    public void init() {
        BasicConfigurator.resetConfiguration();
        PropertyConfigurator.configure(App.class.getResourceAsStream("/log4j.properties"));
        Spark.exception(Exception.class, (exception, request, response) -> exception.printStackTrace());
        staticFiles.location("/public");
        logger.info("start to setup server");
        initMainPage();
        initAPI();
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
     *    @see Torch.QueryType for possible queryType types as value
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

        get("/API/PQ", (req, res) -> {
            logger.info("receive request /API/PQ : {}", req.queryParams("query"));
            return req.queryParams("query") == null ? api.pathQueryByStreetName(req.queryParams("name")) : api.pathQuery(req.queryParams("query"));
        });
        get("/API/SPQ", (req, res) -> {
            logger.info("receive request /API/SPQ : {}", req.queryParams("query"));
            return req.queryParams("query") == null ? api.strictPathQueryByStreetName(req.queryParams("name")) :api.strictPathQuery(req.queryParams("query"));
        });
        get("/API/TKQ", (req, res) -> {
            logger.info("receive request /API/TKQ : {}", req.queryParams("query"));
            return api.similarityQuery(
                    req.queryParams("query"),
                    Integer.parseInt(req.queryParams("k")),
                    req.queryParams("index"),
                    req.queryParams("measure"),
                    req.queryParams("epsilon"));
        });
        get("/API/RQ", (req, res) -> {
            logger.info("receive request /API/RQ : {}", req.queryParams("query"));
            return api.rangeQuery(req.queryParams("query"));
        });
        get("/API/ID", (req, res) -> {
            logger.info("receive request /API/ID : {}", req.queryParams("idSet"));
            return api.resolveIDs(req.queryParams("idSet"));
        });
    }
}
