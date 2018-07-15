package au.edu.rmit.trajectory.similarity.util;

import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Point;
import au.edu.rmit.trajectory.torch.model.TorVertex;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/15/2017
 */
public class GeoUtil {

    private static Logger logger = LoggerFactory.getLogger(GeoUtil.class);

    /**
     * Calculate distance between two points in latitude and longitude taking
     * into account height difference. If you are not interested in height
     * difference pass 0.0. Uses Haversine method as its base.
     * <p>
     * lat1, lon1 Start candidatePoint lat2, lon2 End candidatePoint el1 Start altitude in meters
     * el2 End altitude in meters
     *
     * @return Distance in Meters
     */

    private static double distance(double lat1, double lat2, double lon1, double lon2, double el1, double el2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) + Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters

        double height = el1 - el2;

        distance = Math.pow(distance, 2) + Math.pow(height, 2);

        return Math.sqrt(distance);
    }

    public static double distance(TorVertex v1, TorVertex v2){
        return distance(v1.lat, v2.lat, v1.lng, v2.lng);
    }

    /**
     * @param lat1
     * @param lat2
     * @param lon1
     * @param lon2
     * @return
     */
    public static double distance(double lat1, double lat2, double lon1, double lon2) {
        return distance(lat1, lat2, lon1, lon2, 0.0, 0.0);
    }

    /**
     * @param point1
     * @param point2
     * @return
     */
    public static double distance(GPXEntry point1, GPXEntry point2) {
        return distance(point1.getLat(), point2.getLat(), point1.getLon(), point2.getLon(), 0.0, 0.0);
    }

    public static double distance(MMPoint n1, MMPoint n2) {
        return distance(n1.getLat(), n2.getLat(), n1.getLon(), n2.getLon(), 0.0, 0.0);
    }
    public static double distance(Point point1, Point point2) {
        return distance(point1.getLat(), point2.getLat(), point1.getLon(), point2.getLon(), 0.0, 0.0);
    }

    public static double increaseLatitude(double lat, double meters) {
        double coef = meters * 0.0000089;
        double newLat = lat + coef;
        return newLat;
    }

    public static double increaseLongtitude(double lat, double lon, double meters) {
        double coef = meters * 0.0000089;
        double newLong = lon + coef / Math.cos(lat * 0.018);
        return newLong;
    }

    public static double getLength(List<GPXEntry> pointList) {
        GPXEntry pre = pointList.get(0);
        double length = 0.0;
        for (int i = 1; i < pointList.size(); ++i) {
            length += GeoUtil.distance(pre, pointList.get(i));
            pre = pointList.get(i);
        }
        return length;
    }

    public static double pointToLineDistance(double lat1, double lon1, double lat2, double lon2, double plat, double plon) {
        double y = Math.sin(plon - lon1) * Math.cos(plat);
        double x = Math.cos(lat1) * Math.sin(plat) - Math.sin(lat1) * Math.cos(plat) * Math.cos(plat - lat1);
        double bearing1 = Math.toDegrees(Math.atan2(y, x));
        bearing1 = 360 - ((bearing1 + 360) % 360);

        double y2 = Math.sin(lon2 - lon1) * Math.cos(lat2);
        double x2 = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(lat2 - lat1);
        double bearing2 = Math.toDegrees(Math.atan2(y2, x2));
        bearing2 = 360 - ((bearing2 + 360) % 360);

        double lat1Rads = Math.toRadians(lat1);
        double lat3Rads = Math.toRadians(plat);
        double dLon = Math.toRadians(plon - lon1);

        double distanceAC = Math.acos(Math.sin(lat1Rads) * Math.sin(lat3Rads) + Math.cos(lat1Rads) * Math.cos(lat3Rads) * Math.cos(dLon)) * 6371;
        double min_distance = Math.abs(Math.asin(Math.sin(distanceAC / 6371) * Math.sin(Math.toRadians(bearing1) - Math.toRadians(bearing2))) * 6371);
        return min_distance * 1000;
    }

    public static double getMMLength(List<MMPoint> pointList) {
        MMPoint pre = pointList.get(0);
        double length = 0.0;
        for (int i = 1; i < pointList.size(); ++i) {
            length += GeoUtil.distance(pre, pointList.get(i));
            pre = pointList.get(i);
        }
        return length;
    }

    public static double avgLength(List<GPXEntry> gpxEntryList) {
        double len = 0.0;
        int count = 0;
        GPXEntry pre = null;
        for (GPXEntry gpxEntry : gpxEntryList) {
            if (pre != null) {
                len += distance(pre, gpxEntry);
                ++count;
            }
            pre = gpxEntry;
        }
        return len / count;
    }
}
