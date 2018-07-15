package au.edu.rmit.trajectory.similarity.persistence;

import au.edu.rmit.trajectory.similarity.model.Segment;
import com.graphhopper.util.GPXEntry;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public interface SegmentMapper {

    int insertSegment(Segment segment);

    int insertSegments(List<Segment> segments);

    Segment getSegment(@Param("hash") int id);

    List<Segment> getAllSegment();


}
