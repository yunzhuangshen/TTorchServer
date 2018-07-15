package au.edu.rmit.trajectory.similarity.model;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.EdgeMatch;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.util.PointList;

import java.io.Serializable;
import java.util.*;

/**
 * Edge for pre-computing shortest path
 *
 * @author forrest0402
 * @Description
 * @date 12/4/2017
 */
public class MMEdge implements Edge {

    /**
     * a sum of distance between each point with its adjacent points on edge.
     * metric: meters
     * */
    private double length = Double.MIN_VALUE;

    /**
     * First and last point on edge.
     */
    public MMPoint basePoint, adjPoint;

    /**
     * other points on the edge.
     * */
    private List<MMPoint> pillarPoints = new ArrayList<>();

    public boolean isForward;

    public boolean isBackward;

    /**
     * for database
     */
    private String latitudes;

    /**
     * for database
     */
    private String longtitudes;

    @Override
    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * for LORS, representing the edge position in a trajectory
     */
    private transient int position;

    public void setId(int id) {
        this.id = id;
    }

    /**
     * for database
     */
    private int id = Integer.MIN_VALUE;

    public void addPillarPoint(MMPoint p) {
        p.edge = this;
        this.pillarPoints.add(p);
    }

    public MMEdge() {
    }

    public MMEdge(int id, String latitudes, String longtitudes, double length, boolean isForward, boolean isBackward) {
        this.id = id;
        this.latitudes = latitudes;
        this.longtitudes = longtitudes;
        this.length = length;
        this.isForward = isForward;
        this.isBackward = isBackward;
    }

    public MMEdge(MMEdge edge, int position) {
        this.id = edge.getId();
        this.length = edge.getLength();
        this.position = position;
    }

    public int getId() {
        if (this.id == Integer.MIN_VALUE)
            id = this.hashCode();
        return id;
    }

    /**
     * convert information in fields of this instance, to string.
     * Several fields( e.g. latitudes, longitudes) will be filled in this process.
     *
     * @return an instance of string that contains information that are ready to be processed and stored on disk.
     * @see #convertFromDatabaseForm()
     * */
    public String convertToDatabaseForm() {
        StringBuilder latStringBuilder = new StringBuilder();
        latStringBuilder.append(basePoint.getLat());
        for (MMPoint pillarPoint : pillarPoints) {
            latStringBuilder.append(",").append(pillarPoint.getLat());
        }
        latStringBuilder.append(",").append(adjPoint.getLat());
        this.latitudes = latStringBuilder.toString();

        StringBuilder lonStringBuilder = new StringBuilder();
        lonStringBuilder.append(basePoint.getLon());
        for (MMPoint pillarPoint : pillarPoints) {
            lonStringBuilder.append(",").append(pillarPoint.getLon());
        }
        lonStringBuilder.append(",").append(adjPoint.getLon());
        this.longtitudes = lonStringBuilder.toString();
        getLength();
        if (this.id == Integer.MIN_VALUE)
            this.id = hashCode();

        StringBuilder res = new StringBuilder();
        return res.append(this.id).append(Common.instance.SEPARATOR2)
                .append(this.latitudes).append(Common.instance.SEPARATOR2)
                .append(this.longtitudes).append(Common.instance.SEPARATOR2)
                .append(this.length).append(Common.instance.SEPARATOR2)
                .append(this.isForward).append(Common.instance.SEPARATOR2)
                .append(this.isBackward).append(Common.instance.SEPARATOR2).toString();
    }

    /**
     * after reading data and form an edge with the data in form of string, we need to further process it to
     * make the edge ready to be used.
     *
     * @see #convertFromDatabaseForm()
     * */
    public void convertFromDatabaseForm() {
        String[] lats = this.latitudes.split(",");
        String[] lons = this.longtitudes.split(",");
        int last = lats.length - 1;
        this.basePoint = new MMPoint(Double.parseDouble(lats[0]), Double.parseDouble(lons[0]));
        this.adjPoint = new MMPoint(Double.parseDouble(lats[last]), Double.parseDouble(lons[last]));
        this.pillarPoints.clear();
        for (int i = 1; i < last; ++i) {
            this.pillarPoints.add(new MMPoint(Double.parseDouble(lats[i]), Double.parseDouble(lons[i])));
        }
        getId();
        getLength();
    }

    public MMEdge(AllEdgesIterator edgeMatch, GraphHopper hopper) {
        PointList pointList = edgeMatch.fetchWayGeometry(3);
        basePoint = new MMPoint(pointList.getLat(0), pointList.getLon(0));
        int last = pointList.getSize() - 1;
        adjPoint = new MMPoint(pointList.getLat(last), pointList.getLon(last));
        for (int i = 1; i < last; ++i) {
            pillarPoints.add(new MMPoint(pointList.getLat(i), pointList.getLon(i)));
        }

        this.isForward = edgeMatch.isForward(hopper.getEncodingManager().fetchEdgeEncoders().get(0));
        this.isBackward = edgeMatch.isBackward(hopper.getEncodingManager().fetchEdgeEncoders().get(0));
        this.id = this.hashCode();
    }

    /**
     * This constructor is used for constructing TorSegment right after done the mapmatching work.
     * After trajectory mapped to edges of type EdgeMatch, we are going to model the edges using TorSegment.
     *
     * @param edgeMatch Instance of type EdgeMatch( provided by GraphHopper API), containing information for an edge on the virtual graph.
     * @param hopper Instance of type GraphHopper. To model edges, we need to use it to decode some info in edgeMatch.
     * */
    public MMEdge(EdgeMatch edgeMatch, GraphHopper hopper) {
        PointList pointList = edgeMatch.getEdgeState().fetchWayGeometry(3);

        basePoint = new MMPoint(pointList.getLat(0), pointList.getLon(0));
        int last = pointList.getSize() - 1;
        adjPoint = new MMPoint(pointList.getLat(last), pointList.getLon(last));
        for (int i = 1; i < last; ++i) {
            pillarPoints.add(new MMPoint(pointList.getLat(i), pointList.getLon(i)));
        }
        this.isForward = edgeMatch.getEdgeState().isForward(new CarFlagEncoder());
        this.isForward = edgeMatch.getEdgeState().isForward(hopper.getEncodingManager().fetchEdgeEncoders().get(0));
        this.isBackward = edgeMatch.getEdgeState().isBackward(hopper.getEncodingManager().fetchEdgeEncoders().get(0));
        this.id = this.hashCode();
    }

    public Iterator getPillarPointIterator() {
        return this.pillarPoints.iterator();
    }

    public List<MMPoint> getPillarPoints() {
        return this.pillarPoints;
    }

    public void setPoints(MMPoint basePoint, MMPoint adjPoint) {
        this.basePoint = basePoint;
        this.adjPoint = adjPoint;
    }

    public void setPoints(MMPoint basePoint, MMPoint adjPoint, boolean isForward, boolean isBackward) {
        this.basePoint = basePoint;
        this.adjPoint = adjPoint;
        this.isForward = isForward;
        this.isBackward = isBackward;
    }

    /**
     * @see #length
     * */
    public double getLength() {
        if (length == Double.MIN_VALUE) {
            MMPoint pre = basePoint;
            length = 0.0;
            for (MMPoint pillarPoint : pillarPoints) {
                length += GeoUtil.distance(pre, pillarPoint);
                pre = pillarPoint;
            }
            length += GeoUtil.distance(pre, adjPoint);
        }
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }


    public static String getKey(MMPoint p1, MMPoint p2) {
        return new StringBuilder().append(p1.hashCode()).
                append(Common.instance.SEPARATOR).
                append(p2.hashCode()).toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MMEdge mmEdge = (MMEdge) o;
        boolean flag = true;
        if (pillarPoints.size() == mmEdge.pillarPoints.size() && pillarPoints.size() > 0) {
            flag = pillarPoints.get(0).equals(mmEdge.pillarPoints.get(0));
        }
        return Double.compare(mmEdge.length, length) == 0 &&
                isForward == mmEdge.isForward &&
                isBackward == mmEdge.isBackward &&
                basePoint.equals(mmEdge.basePoint) &&
                adjPoint.equals(mmEdge.adjPoint) &&
                pillarPoints.size() == mmEdge.pillarPoints.size() &&
                flag;
    }

    @Override
    public int hashCode() {
        if (length == Double.MIN_VALUE) {
            getLength();
        }
        if (pillarPoints.size() > 0)
            return Objects.hash(length, basePoint, adjPoint, pillarPoints.get(0), isForward, isBackward);
        return Objects.hash(length, basePoint, adjPoint, isForward, isBackward);
    }
}
