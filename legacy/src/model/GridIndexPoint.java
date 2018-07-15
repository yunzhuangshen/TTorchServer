package au.edu.rmit.trajectory.similarity.model;

import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GridIndexPoint is light weight TorVertex.
 * It represents nodes in grid dataStructure system.
 *
 * @author forrest0402
 * @date 1/3/2018
 */
public class GridIndexPoint implements Point {

    private final float lat, lon;

    private final short position;

    public GridIndexPoint(float lat, float lon, int trajectoryId, short position) {
        this.lat = lat;
        this.lon = lon;
        this.trajectoryId = trajectoryId;
        this.position = position;
    }

    private int trajectoryId;

    public int getTrajectoryId() {
        return trajectoryId;
    }

    public void setTrajectoryId(int trajectoryId) {
        this.trajectoryId = trajectoryId;
    }

    @Override
    public double getLat() {
        return lat;
    }

    @Override
    public double getLon() {
        return lon;
    }

    public short getPosition() {
        return position;
    }
}
