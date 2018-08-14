package au.edu.rmit.bdm.TTorchServer;

public class QueryJson {
    String type;
    String query;

    public QueryJson() {}

    @Override
    public String toString(){
        return "type: " + type + ", query: "+query;
    }
}
