package au.edu.rmit.trajectory.similarity.persistence;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public interface MMEdgeMapper {

    int insertMMEdge(MMEdge segment);

    int insertMMEdges(List<MMEdge> segments);

    MMEdge getMMEdge(@Param("hash") int id);

    List<MMEdge> getAllMMEdges();

    int deleteMMEdges(List<MMEdge> segments);

    int deleteAllMMEdges();
}
