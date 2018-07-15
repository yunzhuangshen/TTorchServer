package au.edu.rmit.trajectory.similarity.service.impl;

import au.edu.rmit.trajectory.similarity.model.Segment;
import au.edu.rmit.trajectory.similarity.persistence.SegmentMapper;
import au.edu.rmit.trajectory.similarity.service.SegmentService;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/23/2017
 */
@Service("SegmentServiceImpl")
public class SegmentServiceImpl implements SegmentService {

    private static Logger logger = LoggerFactory.getLogger(SegmentServiceImpl.class);

    @Autowired
    SegmentMapper segmentMapper;

    @Override
    public int insertSegment(Segment segment) {
        return segmentMapper.insertSegment(segment);
    }

    @Override
    public int insertSegments(List<Segment> segments) {
        return segmentMapper.insertSegments(segments);
    }

    @Override
    public Segment getSegment(@Param("hash") int id) {

        return segmentMapper.getSegment(id);
    }

    @Override
    public List<Segment> getAllSegment() {
        List<Segment> segments = segmentMapper.getAllSegment();
        for (Segment segment : segments) {
            segment.createLonLat();
        }
        return segments;
    }
}
