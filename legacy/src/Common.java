package au.edu.rmit.trajectory.similarity;

import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Segment;
import au.edu.rmit.trajectory.torch.queryEngine.similarity.DistanceFunction;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.util.GPXEntry;

import java.net.URL;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author forrest0402
 * @Description
 * @date 11/11/2017
 */

public enum Common {

    instance;

    public final URL BEIJING_PBF_FILE_PATH = this.getClass().getClassLoader().getResource("map-data/Beijing.osm.pbf");

    public final String STOP_CHARACTOR = "$";

    public final HashMap<String, Integer> ALL_EDGEMATCHES = new HashMap<>();

    public final HashMap<Integer, Segment> ALL_SEGMENTS = new HashMap<>();

    public final List<Segment> SEGMENTS_BULK = new LinkedList<>();

    public final HashMap<String, GPXEntry> ALL_POINTS = new HashMap<>();

    public final MMPoint g = new MMPoint(40.0, -8.0);

    public final ReentrantLock LOCK = new ReentrantLock();

    public final List<Integer> IntBox = new LinkedList<>();

    public final List<Integer> IntBox2 = new LinkedList<>();

    public final List<Integer> IntBox3 = new LinkedList<>();

    public final List<Integer> IntBox4 = new LinkedList<>();

    public final List<Long> LongBox = new LinkedList<>();

    public final List<Long> LongBox2 = new LinkedList<>();

    public final List<Long> LongBox3 = new LinkedList<>();

    public final char SEPARATOR = ',';

    public final String SEPARATOR2 = ";";

    private DistanceFunction<MMPoint, MMPoint> distFunc = (p1, p2) -> GeoUtil.distance(p1, p2);
    private Comparator<MMPoint> comparator = (p1, p2) -> {
        double dist = GeoUtil.distance(p1, p2);
        if (dist < 25) return 0;
        return 1;
    };
    public final SimilarityMeasure<MMPoint> SIM_MEASURE = new SimilarityMeasure<>(distFunc, comparator);

    public final double EPSILON = 20.0;
}

