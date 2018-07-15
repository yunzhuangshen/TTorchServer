package au.edu.rmit.trajectory.similarity.service;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.Segment;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 11/23/2017
 */
public interface MMEdgeService {

    int insertMMEdge(MMEdge edge);

    int insertMMEdges(List<MMEdge> edges);

    MMEdge getMMEdge(@Param("hash") int id);

    List<MMEdge> getAllMMEdges();

    Map<Integer, MMEdge> getAllEdges();

    int deleteMMEdges(List<MMEdge> segments);

    int deleteAllMMEdges();
}
