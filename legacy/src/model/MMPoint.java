package au.edu.rmit.trajectory.similarity.model;

import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.util.GPXEntry;

import java.io.Serializable;
import java.util.*;

/**
 * Point for pre-computing shortest path
 *
 * @author forrest0402
 * @Description
 * @date 12/3/2017
 */
public class MMPoint implements Serializable, Point {

    private transient int id = Integer.MIN_VALUE;

    private double lat;

    private double lon;

    public MMEdge edge;

    private Set<MMPoint> adjPoints = new HashSet<>();//only tower points have adjacent points

    private Map<Integer, Double> adjDistances = new HashMap<>();

    public boolean isTowerPoint = false;

    public double toBasePointDistance = Double.MIN_VALUE;

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        if (this.id == Integer.MIN_VALUE)
            id = this.hashCode();
        return this.id;
    }

    public MMPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public MMPoint(GPXEntry p) {
        this.lat = p.getLat();
        this.lon = p.getLon();
    }

    public MMPoint(MMPoint p1, MMPoint p2, MMEdge edge) {
        this.lat = (p1.getLat() + p2.getLat()) / 2.0;
        this.lon = (p1.getLon() + p2.getLon()) / 2.0;
        this.edge = edge;
    }

    public MMPoint(double lat, double lon, boolean isTowerPoint) {
        this.lat = lat;
        this.lon = lon;
        this.isTowerPoint = isTowerPoint;
    }

    public Iterator<MMPoint> adjIterator() {
        return adjPoints.iterator();
    }

    public void addAdjPoint(MMPoint point) {
        if (!this.adjPoints.contains(point) && !point.equals(this)) {
            this.adjPoints.add(point);
            this.adjDistances.put(point.hashCode(), GeoUtil.distance(point, this));
        }
    }

    public double getAdjDistance(MMPoint point) {
        return this.adjDistances.get(point.hashCode());
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MMPoint point = (MMPoint) o;

        if (Double.compare(point.lat, lat) != 0) return false;
        return Double.compare(point.lon, lon) == 0;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(lat);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(lon);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + lat + ", " + lon + '}';
    }
}
