package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.Torch;
import au.edu.rmit.bdm.TTorch.queryEngine.Engine;
import au.edu.rmit.bdm.TTorch.queryEngine.query.QueryResult;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Spark;
import spark.servlet.SparkApplication;
import static spark.Spark.get;
import static spark.Spark.staticFiles;

public class App implements SparkApplication {
    static Logger logger = LoggerFactory.getLogger(App.class);

    // added to make maven compiler working properly
    public static void main(String[] args){

    }

    private API api;

    @Override
    public void init() {
        BasicConfigurator.resetConfiguration();
        PropertyConfigurator.configure(App.class.getResourceAsStream("/log4j.properties"));
        Spark.exception(Exception.class, (exception, request, response) -> exception.printStackTrace());
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
        get("/API/RQ", (req, res) -> api.rangeQuery(req.params("query")));
    }
}
