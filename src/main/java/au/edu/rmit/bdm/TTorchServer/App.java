package au.edu.rmit.bdm.TTorchServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.servlet.SparkApplication;
import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class App implements SparkApplication {
    Logger logger = LoggerFactory.getLogger(App.class);
    public static void main(String[] args){Spark.init();}    //added to make maven compiler working properly

    private API api;

    @Override
    public void init() {
        System.out.println("abc");
        logger.error("abc");
        Spark.exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
        });
        staticFiles.location("/public");
        initMainPage();
        initAPI();
    }

    private void initMainPage() {
        get("/", (req,res) -> {
            res.redirect("index.html");
            return null;
        });
    }

    private void initAPI() {
        api = new API();
        get("/API/PQ", (req, res) -> api.pathQuery(req.params("query")));
        get("/API/SPQ", (req, res) -> api.strictPathQuery(req.params("query")));
        get("/API/TKQ", (req, res) -> api.similarityQuery( req.params("query"), Integer.parseInt(req.params("k"))));
        get("/API/RQ", (req, res) -> api.rangeQuery(req.params("lng"), req.params("lat"), req.params("radius")));
    }
}
