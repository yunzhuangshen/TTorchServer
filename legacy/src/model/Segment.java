package au.edu.rmit.trajectory.similarity.model;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.matching.EdgeMatch;
import com.graphhopper.matching.GPXExtension;
import com.graphhopper.util.PointList;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author forrest0402
 * @Description
 * @date 11/15/2017
 */
@Component
public class Segment implements Serializable {

    private static final long serialVersionUID = 1L;

    private static transient AtomicInteger idGen = new AtomicInteger(0);

    private int id;

    private double length;

    private double[] latitudeArray;

    private double[] longtitudeArray;

    private String latitudes;

    private String longtitudes;

    public transient List<GPXExtension> gpxExtensions;

    public Segment() {
    }

    public String getKey() {
        if (this.latitudes == null)
            this.latitudes = StringUtils.join(this.latitudeArray, Common.instance.SEPARATOR);
        if (this.longtitudes == null)
            this.longtitudes = StringUtils.join(this.longtitudeArray, Common.instance.SEPARATOR);
        return this.latitudes + this.longtitudes;
    }

    public Segment(EdgeMatch edgeMatch) {

        try {
            Common.instance.LOCK.lock();
            String key = edgeMatch.getEdgeState().toString();
            Integer segmentId = Common.instance.ALL_EDGEMATCHES.get(key);
            if (segmentId != null) {
                Common.instance.ALL_SEGMENTS.get(segmentId).copyTo(this);
            } else {
                this.id = idGen.getAndIncrement();
                Common.instance.ALL_EDGEMATCHES.put(key, this.id);
                Common.instance.ALL_SEGMENTS.put(this.id, this);
                Common.instance.SEGMENTS_BULK.add(this);
                length = 0;
                PointList geoPoints = edgeMatch.getEdgeState().fetchWayGeometry(3);
                this.gpxExtensions = edgeMatch.getGpxExtensions();
                int size = geoPoints.getSize();
                this.latitudeArray = new double[geoPoints.getSize()];
                this.longtitudeArray = new double[geoPoints.getSize()];
                this.latitudeArray[0] = geoPoints.getLat(0);
                this.longtitudeArray[0] = geoPoints.getLon(0);
                for (int i = 1; i < size; ++i) {
                    length += GeoUtil.distance(this.latitudeArray[i - 1], geoPoints.getLat(i), this.longtitudeArray[i - 1], geoPoints.getLon(i));
                    this.latitudeArray[i] = geoPoints.getLat(i);
                    this.longtitudeArray[i] = geoPoints.getLon(i);
                }
                this.latitudes = StringUtils.join(this.latitudeArray, Common.instance.SEPARATOR);
                this.longtitudes = StringUtils.join(longtitudeArray, Common.instance.SEPARATOR);
            }
        } finally {
            Common.instance.LOCK.unlock();
        }
    }

    @Override
    public boolean equals(Object obj) {
        Segment realObj = (Segment) obj;
        return realObj.getID() == this.getID();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("point=")
                .append(latitudeArray[0])
                .append("%2C")
                .append(longtitudeArray[0]);
        for (int i = 1; i < longtitudeArray.length - 1; i += 2) {
//            stringBuilder.append("(")
//                    .append(latitudeArray[i])
//                    .append(", ")
//                    .append(longtitudeArray[i])
//                    .append(")");
            if (i != 0) stringBuilder.append("&point=");
            stringBuilder.append(latitudeArray[i])
                    .append("%2C")
                    .append(longtitudeArray[i]);
        }
        return stringBuilder.toString();
    }

    public double getLength() {
        return length;
    }

    public int getID() {
        return this.id;
    }


    public void createLonLat() {
        getLat(0);
        getLon(0);
    }

    public double getLat(int index) {
        if (this.latitudeArray == null && this.latitudes != null) {
            String[] latitudeString = this.latitudes.split(",");
            this.latitudeArray = new double[latitudeString.length];
            for (int i = 0; i < latitudeString.length; ++i)
                this.latitudeArray[i] = Double.parseDouble(latitudeString[i]);
        }
        if (index < this.latitudeArray.length) return this.latitudeArray[index];
        else throw new ArrayIndexOutOfBoundsException();
    }

    public double getLon(int index) {
        if (this.longtitudeArray == null && this.longtitudes != null) {
            String[] longtitudeString = this.longtitudes.split(",");
            this.longtitudeArray = new double[longtitudeString.length];
            for (int i = 0; i < longtitudeString.length; ++i)
                this.longtitudeArray[i] = Double.parseDouble(longtitudeString[i]);
        }
        if (index < this.longtitudeArray.length) return this.longtitudeArray[index];
        else throw new ArrayIndexOutOfBoundsException();
    }

    public int size() {
        return this.longtitudeArray.length;
    }

    private void copyTo(Segment segment) {
        segment.id = this.id;
        segment.length = this.length;
        segment.latitudeArray = this.latitudeArray;
        segment.longtitudeArray = this.longtitudeArray;
    }

    public double distanceTo(Segment segment) {
        return (GeoUtil.distance(this.getLat(0), segment.getLat(0),
                this.getLon(0), segment.getLon(0)) +
                GeoUtil.distance(this.getLat(this.size() - 1), segment.getLat(segment.size() - 1),
                        this.getLon(this.size() - 1), segment.getLon(segment.size() - 1))) / 2.0;

    }

}
