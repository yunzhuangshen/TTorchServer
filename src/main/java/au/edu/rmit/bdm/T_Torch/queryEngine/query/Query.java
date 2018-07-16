package au.edu.rmit.bdm.T_Torch.queryEngine.query;

import au.edu.rmit.bdm.T_Torch.base.model.TrajEntry;

import java.util.List;

public interface Query {

    /**
     * For query of type windowQuery, param passed in should be an object of type SearchWindow as to specify the search range.<p>
     * For query of type TopKQuery, param passed in should be an object of type Integer as to specify the number of results retrieved.<p>
     * For the rest query types, null value is expected.
     *
     * @param param A SearchWindow object indicates the range to search on.
     * @return A list of trajectories meets the specific query requirement.
     * @throws IllegalStateException if the passed object type is not expected for an particular query, exception will be thrown.
     */
    QueryResult execute(Object param);

    /**
     * If search on the map-matched bdm set, the query bdm will also be converted to map-matched bdm.<p>
     * If search on the raw bdm set, the query bdm will be unchanged.
     *
     * @param raw the query bdm
     * @return true if the query bdm mapped successfully, or no map-matching is required.
     *         false if the query bdm cannot be mapped properly.
     */
     boolean prepare(List<? extends TrajEntry> raw);
}
