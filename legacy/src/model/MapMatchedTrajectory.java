package au.edu.rmit.trajectory.similarity.model;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.service.SegmentService;
import au.edu.rmit.trajectory.similarity.service.impl.SegmentServiceImpl;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public class MapMatchedTrajectory implements Serializable {

    private static final long serialVersionUID = 1L;

    private double invertedScore = 0;

    public List<Segment> getSegments() {
        return segments;
    }

    /**
     * Convert segmentIDs to Segment classes
     *
     * @param segmentMap
     */
    public void createSegments(Map<Integer, Segment> segmentMap) {
        String[] idString = segmentIDs.split(",");
        this.segments = new ArrayList<>(idString.length);
        for (String s : idString) {
            int segmentId = Integer.parseInt(s);
            this.segments.add(segmentMap.get(segmentId));
        }
    }

    private List<Segment> segments;

    private int id;

    private int trajectoryId;

    private String segmentIDs;

    public Segment get(int index) {
        if (index < segments.size()) return segments.get(index);
        else return null;
    }

    public int size() {
        return segments.size();
    }

    public MapMatchedTrajectory() {
    }

    public MapMatchedTrajectory(List<Segment> segments, int id, int trajectoryId) {
        this.segments = segments;
        this.id = id;
        this.trajectoryId = trajectoryId;
        List<Integer> ids = new ArrayList<>(segments.size());
        for (Segment segment : segments) {
            ids.add(segment.getID());
        }
        this.segmentIDs = StringUtils.join(ids, Common.instance.SEPARATOR);
    }

    public Iterator iterator() {
        return segments.iterator();
    }

    public String getSegmentIDs() {
        return segmentIDs;
    }

    public void setSegmentIDs(String segmentIDs) {
        this.segmentIDs = segmentIDs;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getTrajectoryId() {
        return trajectoryId;
    }

    public void setTrajectoryId(int trajectoryId) {
        this.trajectoryId = trajectoryId;
    }

    public double getInvertedScore() {
        return invertedScore;
    }

    public void setInvertedScore(double invertedScore) {
        this.invertedScore = invertedScore;
    }
}
