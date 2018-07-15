package au.edu.rmit.trajectory.similarity.task.formatter;

import au.edu.rmit.trajectory.similarity.model.MMPoint;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * see https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i/data for detail
 *
 * @author forrest0402
 * @Description
 * @date 11/17/2017
 */
@Component("TrajectoryStringFormatter")
public class TrajectoryStringFormatter extends LineFormatter {

    private static Logger logger = LoggerFactory.getLogger(TrajectoryStringFormatter.class);

    /**
     * @see LineFormatter#format(String)
     * */
    @Override
    public List<GPXEntry> format(String lineStr) {
        try {
            List<GPXEntry> entryList = new LinkedList<>();
            String[] pointArray = lineStr.split("],\\[");
            for (String str : pointArray) {
                double lat = Double.parseDouble(str.replace("[", "").replace("]", "").split(",")[1]);
                double lon = Double.parseDouble(str.replace("[", "").replace("]", "").split(",")[0]);

                //String key = String.format("%f%f", lat, lng);
                GPXEntry entry = null; //Common.instance.ALL_POINTS.get(key);
                long millis = 0;
                if (entryList.size() > 0) {
                    GPXEntry lastGPXEntry = entryList.get(entryList.size() - 1);
                    millis = lastGPXEntry.getTime();

                    // distance / speed = time to traverse from A to B, we further converse the metric to milliseconds.
                    //millis += Math.round(calcDist(lastGPXEntry.getLat(), lastGPXEntry.getLon(), lat, lng) / defaultSpeed * 3600);
                }
                entry = new GPXEntry(lat, lon, millis);
                entryList.add(entry);
            }
            return entryList;
        } catch (Exception e) {
            logger.error("lineStr={}, error:{}", lineStr, e);
            return null;
        }
    }


    /**
     * @see LineFormatter#mmFormat(String)
     * */
    @Override
    public List<MMPoint> mmFormat(String lineStr) {
        try {
            List<MMPoint> trajectory = new LinkedList<>();

            //split the string by pattern of "],["
            String[] pointArray = lineStr.split("],\\[");
            for (String str : pointArray) {
                double lat = Double.parseDouble(str.replace("[", "").replace("]", "").split(",")[1]);
                double lon = Double.parseDouble(str.replace("[", "").replace("]", "").split(",")[0]);

                MMPoint entry = new MMPoint(lat, lon);
                trajectory.add(entry);
            }
            return trajectory;
        } catch (Exception e) {
            logger.error("lineStr={}, error:{}", lineStr, e);
            return null;
        }
    }
}
