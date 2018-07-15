package au.edu.rmit.trajectory.similarity.service;

import au.edu.rmit.trajectory.similarity.model.Segment;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/23/2017
 */
public interface SegmentService {

    int insertSegment(Segment segment);

    int insertSegments(List<Segment> segments);

    Segment getSegment(@Param("hash") int id);

    List<Segment> getAllSegment();

}
