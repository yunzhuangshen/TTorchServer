package au.edu.rmit.trajectory.similarity.model;

/**
 *
 * ComplexGridIndexPoint is light weight TorVertex.
 * It represents nodes in complex grid dataStructure system.
 *
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
public class ComplexGridIndexPoint {

    private final int pointID;

    private int trajectoryId;

    public ComplexGridIndexPoint(int trajectoryId, int pointID) {
        this.trajectoryId = trajectoryId;
        this.pointID = pointID;
    }

    public int getTrajectoryId() {
        return trajectoryId;
    }

    public void setTrajectoryId(int trajectoryId) {
        this.trajectoryId = trajectoryId;
    }

//    @Override
//    public double getLat() {
//        return lat;
//    }
//
//    @Override
//    public double getLon() {
//        return lng;
//    }

    public int getPointID() {
        return pointID;
    }
}
