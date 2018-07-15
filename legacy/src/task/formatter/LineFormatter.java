package au.edu.rmit.trajectory.similarity.task.formatter;

import au.edu.rmit.trajectory.similarity.model.MMPoint;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.lang.Math.cos;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

/**
 * @author forrest0402
 * @Description
 * @date 11/17/2017
 */
public abstract class LineFormatter {

    /**
     *  convert a string containing points information of trajectories to a list of instances of type GPXEntry
     *  string format: [[lng1,lat1, time1], [lng2,lat2, time2], [lng3,lat3, time3]...]
     * */
    public abstract List<GPXEntry> format(String lineStr);

    /**
     *  convert a string containing points information of trajectories to a list of instances of type TorVertex
     *  string format: [[lng1,lat1], [lng2,lat2], [lng3,lat3]...]
     * */
    public abstract List<MMPoint> mmFormat(String lineStr);
}
