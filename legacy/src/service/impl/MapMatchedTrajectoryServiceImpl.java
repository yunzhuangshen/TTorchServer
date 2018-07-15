package au.edu.rmit.trajectory.similarity.service.impl;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.model.MapMatchedTrajectory;
import au.edu.rmit.trajectory.similarity.model.Segment;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.persistence.MapMatchedTrajectoryMapper;
import au.edu.rmit.trajectory.similarity.persistence.SegmentMapper;
import au.edu.rmit.trajectory.similarity.service.MapMatchedTrajectoryService;
import au.edu.rmit.trajectory.similarity.service.SegmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Service("MapMatchedTrajectoryService")
public class MapMatchedTrajectoryServiceImpl implements MapMatchedTrajectoryService {

    private static Logger logger = LoggerFactory.getLogger(MapMatchedTrajectoryServiceImpl.class);

    @Autowired
    MapMatchedTrajectoryMapper mapMatchedTrajectoryMapper;

    @Autowired
    SegmentMapper segmentMapper;

    @Autowired
    SegmentService segmentService;

    @Override
    public List<MapMatchedTrajectory> getAllMapMatchedTrajectory() {
        List<MapMatchedTrajectory> mapMatchedTrajectories = mapMatchedTrajectoryMapper.getAllMapMatchedTrajectory();
        if (Common.instance.ALL_SEGMENTS != null && Common.instance.ALL_SEGMENTS.size() > 0) {
            for (MapMatchedTrajectory mapMatchedTrajectory : mapMatchedTrajectories) {
                mapMatchedTrajectory.createSegments(Common.instance.ALL_SEGMENTS);
            }
        } else {
            List<Segment> segments = segmentService.getAllSegment();
            Map<Integer, Segment> segmentMap = new HashMap<>();
            for (Segment segment : segments) {
                segmentMap.put(segment.getID(), segment);
            }
            for (MapMatchedTrajectory mapMatchedTrajectory : mapMatchedTrajectories) {
                mapMatchedTrajectory.createSegments(segmentMap);
            }
            segments.clear();
            segmentMap.clear();
        }
        return mapMatchedTrajectories;
    }

    @Override
    public MapMatchedTrajectory getMapMatchedTrajectory(int id) {
        return mapMatchedTrajectoryMapper.getMapMatchedTrajectory(id);
    }

    @Override
    public int insertMapMatchedTrajectory(MapMatchedTrajectory mapMatchedTrajectory) {
        return mapMatchedTrajectoryMapper.insertMapMatchedTrajectory(mapMatchedTrajectory);
    }

    @Override
    public int insertMapMatchedTrajectories(List<MapMatchedTrajectory> mapMatchedTrajectories) {
        //segmentMapper
        return mapMatchedTrajectoryMapper.insertMapMatchedTrajectories(mapMatchedTrajectories);
    }
}
