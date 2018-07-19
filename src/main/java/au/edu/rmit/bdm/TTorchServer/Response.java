package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.queryEngine.query.QueryResult;
import com.google.gson.Gson;

class Response {
    static Gson gson = new Gson();

    boolean formatCorrect;
    String queryRet;

    private Response(){
        formatCorrect = false;
        queryRet = null;
    }

    private Response(QueryResult queryResult){
        formatCorrect = true;
        this.queryRet = queryRet;    }

    static Response genFailed(){
        return new Response();
    }

    static Response genSuccessful(QueryResult ret){
        return new Response(ret);
    }

    String toJSON(){
        return gson.toJson(this);
    }
}
