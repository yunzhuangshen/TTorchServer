package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 12/18/2017
 */
@Component
public class Transformation {

    private static Logger logger = LoggerFactory.getLogger(Transformation.class);

    private static SecureRandom random = new SecureRandom();

    final static double CAR_SPEED = 20.0;//20 km/h

    /**
     * For each trajectory, consider a point obey Gaussian distribution #adding 10% number of points with distance to original points less than errorSigma
     *
     * @param _points
     * @param errorSigma
     * @return
     */
    public List<MMPoint> randomShift(List<MMPoint> _points, double errorSigma) {
        List<MMPoint> points = new ArrayList<>();
        for (MMPoint point : _points) {
            points.add(new MMPoint(point.getLat(), point.getLon()));
        }
        for (MMPoint point : points) {
            double latOffset = random.nextInt() % 2 == 0 ? -errorSigma : errorSigma;
            double lonOffset = random.nextInt() % 2 == 0 ? -errorSigma : errorSigma;
            point.setLat(GeoUtil.increaseLatitude(point.getLat(), latOffset));
            point.setLon(GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), lonOffset));
        }
        return points;
    }


    /**
     * move every point with a distance along the road
     *
     * @param _points
     * @param distance
     * @return
     */
    public List<MMPoint> shifting(List<MMPoint> _points, double distance) {
        List<MMPoint> points = new ArrayList<>();
        for (MMPoint point : _points) {
            points.add(new MMPoint(point.getLat(), point.getLon()));
        }
        MMPoint point = new MMPoint(_points.get(0).getLat(), _points.get(0).getLon());
        for (int i = 1; i < points.size() - 1; ++i) {
            double dist = GeoUtil.distance(points.get(i), points.get(i + 1));
            if (i + 1 == points.size() - 1 && dist < distance) break;
            double percentage = (dist - distance) / dist;
            double latOffset = points.get(i).getLat() + (points.get(i + 1).getLat() - points.get(i).getLat()) * percentage;
            double lonOffset = points.get(i).getLon() + (points.get(i + 1).getLon() - points.get(i).getLon()) * percentage;
            points.get(i).setLat(latOffset);
            points.get(i).setLon(lonOffset);
        }
        double dist = GeoUtil.distance(points.get(0), points.get(1));
        if (dist >= 900) {
            points.add(1, point);
        }
        return points;
    }

    private MMPoint addPoint(List<MMPoint> points, int curPos, double distance) {
        int nextPos = (curPos + 1) % points.size();
        if (nextPos == 0) {
            return addPoint(points, nextPos, distance);
        }
        double gap = GeoUtil.distance(points.get(curPos), points.get(nextPos));
        if (gap < distance) {
            return addPoint(points, nextPos, distance - gap);
        } else {
            double percentage = (gap - distance) / gap;
            double latOffset = points.get(curPos).getLat() + (points.get(nextPos).getLat() - points.get(curPos).getLat()) * percentage;
            double lonOffset = points.get(curPos).getLon() + (points.get(nextPos).getLon() - points.get(curPos).getLon()) * percentage;
            return new MMPoint(latOffset, lonOffset);
        }
    }

    public List<MMPoint> shifting2(List<MMPoint> _points, double distance) {
        List<MMPoint> points = new ArrayList<>();
        for (int i = 0; i < _points.size(); ++i) {
            MMPoint point = addPoint(_points, i, distance);
            points.add(point);
        }
        return points;
    }

    /**
     * a time-based sampling method
     *
     * @param _points
     * @param period  unit seconds
     * @return
     */
    public List<MMPoint> reSampling(List<MMPoint> _points, long period) {
        if (_points.size() < 2) throw new IllegalArgumentException("the trajectory is too short.");
        List<MMPoint> points = new ArrayList<>();
        for (MMPoint point : _points) {
            points.add(new MMPoint(point.getLat(), point.getLon()));
        }
        MMPoint pre = points.get(0);
        MMPoint endPoint = points.get(points.size() - 1);
        List<MMPoint> res = new ArrayList<>();
        res.add(pre);
        double distance = period / 3600.0 * CAR_SPEED * 1000.0; // unit meters
        double curDistance = 0.0;
        for (int i = 1; i < points.size() - 1; ) {
            if (points.get(i).isTowerPoint) {// a sample point is found
                res.add(points.get(i));
                curDistance = 0;
                pre = points.get(i++);
                continue;
            }
            double dist = GeoUtil.distance(pre, points.get(i));
            if (dist + curDistance > distance) { // a sample point is found
                double percentage = (distance - curDistance) / dist;
                double latOffset = pre.getLat() + (points.get(i).getLat() - pre.getLat()) * percentage;
                double lonOffset = pre.getLon() + (points.get(i).getLon() - pre.getLon()) * percentage;
                MMPoint samplePoint = new MMPoint(latOffset, lonOffset);
                res.add(samplePoint);
                pre = samplePoint;
                curDistance = 0.0;
            } else {
                curDistance += dist;
                pre = points.get(i);
                ++i;
            }
        }
        res.add(endPoint);
        return res;
    }

    public static void printPoints(List<MMPoint> pointList) {
        System.out.print("https://graphhopper.com/maps/?");
        for (MMPoint mmPoint : pointList) {
            System.out.print("&point=" + mmPoint.getLat() + "%2C" + mmPoint.getLon());
        }
        System.out.println();
    }

    public static void printEdges(List<MMEdge> edgeList) {
        System.out.print("https://graphhopper.com/maps/?");
        for (MMEdge edge : edgeList) {
            System.out.print("&point=" + edge.basePoint.getLat() + "%2C" + edge.basePoint.getLon());
        }
        System.out.println("");
    }
}
