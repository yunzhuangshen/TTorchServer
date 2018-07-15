package au.edu.rmit.trajectory.similarity.model;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.task.formatter.LineFormatter;
import au.edu.rmit.trajectory.similarity.task.formatter.TrajectoryStringFormatter;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.util.GPXEntry;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public class Trajectory implements Serializable {

    private static final long serialVersionUID = 1L;

    private static LineFormatter lineFormatter = new TrajectoryStringFormatter();

    private transient List<GPXEntry> points = new LinkedList<>();

    private transient List<MMPoint> mmPoints = new LinkedList<>();

    private transient List<MMPoint> matchedPoints = new LinkedList<>();

    private transient List<MMPoint> matchedAlignedPoints = new LinkedList<>();



    private transient double length = Double.MIN_VALUE;

    private List<MMEdge> edges = new LinkedList<>();

    private transient final float minLat = 41.108936f, maxLat = 41.222659f, minLon = -8.704896f, maxLon = -8.489324f;

    private int id;

    private String pointStr;

    public String convertToDatabaseForm() {
        StringBuilder res = new StringBuilder().append(this.id).append("\t").
                append(this.pointStr).append("\t").append(edgeStr);
        return res.toString();
    }

    /**
     * construct instances of TorSegment mapping to this trajectory.
     * using edgeStr( id1, id2, id3...) and edgeid-TorSegment map.
     *
     * @param allEdges map object contains entry with (edgeId - all edges in Beijing).
     * @return a list of instances of type TorSegment mapping to this trajectory.
     */
    public synchronized List<MMEdge> getMapMatchedTrajectory(Map<Integer, MMEdge> allEdges) {
        if (edges.size() == 0 && this.edgeStr != null) {
            String[] array = edgeStr.split(String.valueOf(Common.instance.SEPARATOR));
            try {
                for (String s : array) {
                    MMEdge edge = allEdges.get(Integer.parseInt(s));
                    if (edge != null)
                        edges.add(edge);
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            //this.edgeStr = null;
        }
        return edges;
    }

    /**
     * Change the getMMPoints() to calibrated points
     */
    public synchronized void calibrate() {
        this.mmPoints = getMatchedPoints();
    }

    /**
     * get matched points from edges.
     *
     * @return representative points on virtual graph, representing this trajectory.
     */
    public synchronized List<MMPoint> getMatchedPoints() {
        if (matchedPoints.size() == 0) {
            if (this.edges.size() == 0)
                throw new IllegalStateException("invoke getMapMatchedTrajectory first");
            matchedPoints = getAllTowerPoints(this.edges);
        }
        return matchedPoints;
    }

    /**
     * select representative points from edges, used for representing this trajectory
     *
     * @param edges a list of instances of type TorSegment, representing this trajectory
     * @return a list of 'representative' points representing the trajectory
     */
    private List<MMPoint> getAllTowerPoints(List<MMEdge> edges) {
        List<MMPoint> points = new ArrayList<>();
        if (edges == null || edges.size() == 0) return points;
        if (edges.size() == 1) {
            MMEdge edge = edges.get(0);
            points.add(edge.basePoint);
            points.add(edge.adjPoint);
            return points;
        }
        for (int i = 0; i < edges.size() - 1; ++i) {
            MMEdge cur = edges.get(i);
            if (cur.adjPoint == edges.get(i + 1).adjPoint || cur.adjPoint == edges.get(i + 1).basePoint) {
                points.add(cur.basePoint);
            } else {
                points.add(cur.adjPoint);
            }
        }
        MMEdge lastEdge = edges.get(edges.size() - 1);
        if (lastEdge.basePoint == edges.get(edges.size() - 2).adjPoint
                || lastEdge.basePoint == edges.get(edges.size() - 2).basePoint) {
            points.add(lastEdge.basePoint);
            points.add(lastEdge.adjPoint);
        } else {
            points.add(lastEdge.adjPoint);
            points.add(lastEdge.basePoint);
        }
        return points;
    }

    public void setEdgeStr(String edgeStr) {
        this.edgeStr = edgeStr;
    }

    private String edgeStr;

    public GPXEntry get(int index) {
        if (index < points.size()) return points.get(index);
        else return null;
    }

    public synchronized boolean isShort(double length) {
        if (this.length == Double.MIN_VALUE) {
            List<GPXEntry> pointList = getPoints();
            GPXEntry pre = pointList.get(0);
            this.length = 0.0;
            for (int i = 1; i < pointList.size(); ++i) {
                this.length += GeoUtil.distance(pre, pointList.get(i));
                pre = pointList.get(i);
                if (this.length >= length) {
                    this.length = Double.MIN_VALUE;
                    return false;
                }
            }
        }
        return this.length < length;
    }

    public boolean isValidQuery() {
        boolean flag = this.edgeStr != null;
        flag = getTotalLength() < 10000;
        if (flag) {
            for (GPXEntry point : getPoints()) {
                if (point.getLat() < minLat || point.getLat() > maxLat || point.getLon() < minLon || point.getLon() > maxLon) {
                    flag = false;
                    break;
                }
            }
        }
        return flag;
    }

    /**
     * @return sum of length of edges( distance from one side of the edge to the other side) representing the trajectory.
     *         metric: meter
     * */
    public synchronized double getTotalLength() {
        if (this.length == Double.MIN_VALUE) {
            List<MMPoint> pointList = getMMPoints();
            MMPoint pre = pointList.get(0);
            this.length = 0.0;
            for (int i = 1; i < pointList.size(); ++i) {
                this.length += GeoUtil.distance(pre, pointList.get(i));
                pre = pointList.get(i);
            }
        }
        return this.length;
    }

    public int size() {
        if (this.mmPoints.size() != 0) return this.mmPoints.size();
        if (this.points.size() == 0 && this.pointStr != null) {
            getPoints();
        }
        return points.size();
    }

    public Trajectory() {
    }

    public Trajectory(int id, List<MMPoint> pointList) {
        this.id = id;
        this.mmPoints = pointList;
    }

    public Trajectory(int id, List<MMPoint> pointList, String edgeStr) {
        this.id = id;
        this.mmPoints = pointList;
        this.edgeStr = edgeStr;
    }

    public Trajectory(int id, String pointStr, String edgeStr) {
        this.id = id;
        this.pointStr = pointStr;
        this.edgeStr = edgeStr;
    }

    public Trajectory(List<GPXEntry> points) {
        this.points = points;
    }

    public Trajectory(String pointStr) {
        this.pointStr = pointStr;
    }

    public Iterator iterator() {
        return points.iterator();
    }

    public String getPointStr() {
        return pointStr;
    }

    public void setPointStr(String pointStr) {
        this.pointStr = pointStr;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getEdgeStr() {
        return this.edgeStr;
    }

    public synchronized List<MMPoint> getMatchedAlignedPoints() {
        if (matchedAlignedPoints.size() == 0) {
            if (this.edges.size() == 0)
                throw new IllegalStateException("invoke getMapMatchedTrajectory first");
            matchedAlignedPoints = getAllPoints(this.edges);
        }
        return matchedAlignedPoints;
    }

    /**
     * get a list of instances of type TorVertex, which are (lat, lng) gps coordinate on this trajectory.
     * if mmPoints array is of size 0, which means the trajectory is just instantiated, then generate it from the string form.
     *
     * @return a list of instances of type TorVertex
     * */
    public synchronized List<MMPoint> getMMPoints() {
        if (this.mmPoints.size() == 0 && this.pointStr != null) {
            this.mmPoints = lineFormatter.mmFormat(this.pointStr);
            this.pointStr = null;
        } else if (this.mmPoints.size() == 0) {
            List<GPXEntry> gpxEntryList = getPoints();
            for (GPXEntry gpxEntry : gpxEntryList) {
                this.mmPoints.add(new MMPoint(gpxEntry));
            }
        }
        return this.mmPoints;
    }

    /**
     * get a list of instances of type GPXEntry, which are (lat, lng) gps coordinate on this trajectory.
     * if points array is of size 0, which means the trajectory is just instantiated, then generate it from the string form.
     *
     * @return a list of instances of type GPXEntry
     * */
    public synchronized List<GPXEntry> getPoints() {
        if (this.points.size() == 0 && this.pointStr != null) {
            this.points = lineFormatter.format(this.pointStr);
            this.pointStr = null;
        }
        return this.points;
    }

    public void calibrate(Mapper mapper) {
        mmPoints.clear();
        mapper.fastMatch(getPoints(), this.mmPoints, new ArrayList<>());
        this.points = null;
        this.pointStr = null;
    }

    public void clear() {
        this.mmPoints = null;
        this.points = null;
        this.pointStr = null;
    }

    private List<MMPoint> getAllPoints(List<MMEdge> trajectory) {
        List<MMPoint> points = new ArrayList<>();
        if (trajectory == null || trajectory.size() == 0) return points;
        if (trajectory.size() == 1) {
            MMEdge edge = trajectory.get(0);
            points.add(edge.basePoint);
            for (MMPoint point : edge.getPillarPoints()) {
                points.add(point);
            }
            points.add(edge.adjPoint);
            return points;
        }
        for (int i = 0; i < trajectory.size() - 1; ++i) {
            MMEdge cur = trajectory.get(i);
            if (cur.adjPoint == trajectory.get(i + 1).adjPoint || cur.adjPoint == trajectory.get(i + 1).basePoint) {
                points.add(cur.basePoint);
                for (MMPoint point : cur.getPillarPoints()) {
                    points.add(point);
                }
            } else {
                points.add(cur.adjPoint);
                for (int j = cur.getPillarPoints().size() - 1; j >= 0; --j) {
                    points.add(cur.getPillarPoints().get(j));
                }
            }
        }
        MMEdge lastEdge = trajectory.get(trajectory.size() - 1);
        if (lastEdge.basePoint == trajectory.get(trajectory.size() - 2).adjPoint
                || lastEdge.basePoint == trajectory.get(trajectory.size() - 2).basePoint) {
            points.add(lastEdge.basePoint);
            for (MMPoint point : lastEdge.getPillarPoints()) {
                points.add(point);
            }
            points.add(lastEdge.adjPoint);
        } else {
            points.add(lastEdge.adjPoint);
            for (int j = lastEdge.getPillarPoints().size() - 1; j >= 0; --j) {
                points.add(lastEdge.getPillarPoints().get(j));
            }
            points.add(lastEdge.basePoint);
        }
        return points;
    }

    /**
     * serialize a list of points to string in the format of [[lat,lng],[lat,lng]...]
     * */
    public static String convertToPointStr(List<MMPoint> points) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[");
        boolean first = true;
        for (MMPoint point : points) {
            if (first)
                first = false;
            else stringBuilder.append(",");
            stringBuilder.append("[").append(point.getLon()).append(",").append(point.getLat()).append("]");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        if (edgeStr != null)
            return id + "\t" + pointStr + '\t' + edgeStr;
        return id + "\t" + pointStr;
    }
}
