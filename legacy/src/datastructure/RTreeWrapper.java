package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.github.davidmoten.rtree.*;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.github.davidmoten.rtree.geometry.Rectangle;
import com.graphhopper.util.GPXEntry;
import com.javamex.classmexer.MemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * divide trajectories into envelope and use R tree to dataStructure them
 *
 * @author forrest0402
 * @Description
 * @date 12/15/2017
 */
@Component
public class RTreeWrapper {

    private static Logger logger = LoggerFactory.getLogger(RTreeWrapper.class);

    @Autowired
    TrajectoryService trajectoryService;

    private final double GPS_ERROR_SIGMA = 0.5;

    private final String TRAJ_ENVELOPE_PATH = "dataStructure/Envelope";

    private final String INDEX_PATH = "dataStructure/R-Tree.dataStructure";

    private final String TRAJ_ID_PATH = "exp/effectiveness/trajectory.hash.200000.txt";

    private final double MAX_LENGTH = 500;

    /**
     * This is the threashold for number of points in MBR( Envelope)
     * If a trajectory contains more than 10 points,
     * it will be sliced into multiple MBR( Envelope)
     */
    private final int POINT_NUMBER_IN_MBR = 10;

    private RTree<Integer, Geometry> rTree;

    private Map<Integer, List<Rectangle>> trajectoryMBRMap;

    /**
     * write envelope(node of RTree) to file.
     * @param validTrajEnvelopes a list of envelopes.
     * @param file output file to save envelopes.
     */
    private void saveEnvelope(List<Envelope> validTrajEnvelopes, File file) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (Envelope validTrajEnvelope : validTrajEnvelopes) {
                writer.write(validTrajEnvelope.toString());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    private void calUpperBound(rx.Observable<Entry<Integer, Geometry>> results, Map<Integer, Double> idScores, final double pointNumber) {
        results.forEach(entry -> {
            Double value = idScores.get(entry.value());
            if (value == null)
                idScores.put(entry.value(), pointNumber);
            else idScores.put(entry.value(), pointNumber + value);
        });
    }

    private void findMBRs(rx.Observable<Entry<Integer, Geometry>> results, Set<Integer> trajectoryID) {
        results.forEach(entry -> {
            trajectoryID.add(entry.value());
        });
    }

    private double calDistanceBtwRect(Rectangle q, Rectangle r) {
        float[] qlon = new float[]{q.x1(), q.x2()};
        float[] qlat = new float[]{q.y1(), q.y2()};

        float[] rlon = new float[]{r.x1(), r.x2()};
        float[] rlat = new float[]{r.y1(), r.y2()};

        double minDistance = Double.MAX_VALUE;
        for (float lon1 : qlon) {
            for (float lat1 : qlat) {
                for (float lon2 : rlon) {
                    for (float lat2 : rlat) {
                        double distance = GeoUtil.distance(lat1, lat2, lon1, lon2);
                        if (distance < minDistance) minDistance = distance;
                    }
                }
            }
        }
        return minDistance;
    }

    private double calLowerBound(List<Rectangle> q, List<Rectangle> r) {

        if (q.size() == 0 || r.size() == 0)
            throw new IllegalArgumentException("q size: " + q.size() + ", r size: " + r.size() + " cannot be zero.");

        double lowerBound = 0;

        if (q.size() < r.size()) {
            List<Rectangle> temp = q;
            q = r;
            r = temp;
        }

        for (int i = 0; i < r.size(); ++i) {
            lowerBound += calDistanceBtwRect(q.get(i), r.get(i));
        }

        for (int i = r.size(); i < q.size(); ++i) {
            lowerBound += calDistanceBtwRect(q.get(i), r.get(r.size() - 1));
        }

        return lowerBound;
    }

    public List<Integer> findTopK(Map<Integer, Trajectory> trajectoryMap, Trajectory trajectory, int k, List<Integer> candidateNumberList, List<Integer> scannedCandidateNumberList) {
        if (this.rTree == null)
            throw new IllegalArgumentException("call buildRTree() first");
        List<MMPoint> points = trajectory.getMMPoints();
        MMPoint pre = points.get(0);
        double minLat = pre.getLat(), minLon = pre.getLon(), maxLat = pre.getLat(), maxLon = pre.getLon();
        double pointNumber = 0;
        //key for trajectory hash, value for MBR
        Set<Integer> trajectoryID = new HashSet<>();
        List<Rectangle> queryMBE = new ArrayList<>();
        for (int i = 1; i < points.size(); ++i) {
            pre = points.get(i);
            ++pointNumber;
            if (pointNumber >= POINT_NUMBER_IN_MBR || i == points.size() - 1) {
                Rectangle mbr = Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat);
                rx.Observable<Entry<Integer, Geometry>> results = this.rTree.search(mbr);
                queryMBE.add(mbr);
                findMBRs(results, trajectoryID);
                minLat = pre.getLat();
                minLon = pre.getLon();
                maxLat = pre.getLat();
                maxLon = pre.getLon();
                pointNumber = 0;
                ++i;
            }
            if (pre.getLat() > maxLat) maxLat = pre.getLat();
            if (pre.getLat() < minLat) minLat = pre.getLat();
            if (pre.getLon() > maxLon) maxLon = pre.getLon();
            if (pre.getLon() < minLon) minLon = pre.getLon();
        }


        rx.Observable<Entry<Integer, Geometry>> results = this.rTree.search(
                Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat));
        findMBRs(results, trajectoryID);
        //find real candidate
        PriorityQueue<Pair> rankedCandidates = new PriorityQueue<>((p1, p2) -> Double.compare(p1.lowerBound, p2.lowerBound));
        for (Integer id : trajectoryID) {
            List<Rectangle> candidateMBE = trajectoryMBRMap.get(id);
            double lowerBound = calLowerBound(queryMBE, candidateMBE);
            rankedCandidates.add(new Pair(id, lowerBound));
        }
        int candidateNumber = rankedCandidates.size();

        //calculate the distance between the trajectory and the query
        PriorityQueue<Pair> topkHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p2.lowerBound, p1.lowerBound));
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        double bestSoFar = 0;
        int scannedCandidateNumber = 0;
        while (rankedCandidates.size() > 0) {
            Pair pair = rankedCandidates.poll();
            if (pair.lowerBound > bestSoFar && topkHeap.size() >= k) break;
            ++scannedCandidateNumber;
            pair.lowerBound = similarityMeasure.fastDynamicTimeWarping(trajectoryMap.get(pair.trajectoryID).getMMPoints(), trajectory.getMMPoints(), 10, bestSoFar, null);
            topkHeap.add(pair);
            if (topkHeap.size() > k) topkHeap.poll();
        }

        //return the results
        List<Integer> resIDList = new ArrayList<>();
        while (topkHeap.size() > 0) {
            resIDList.add(topkHeap.poll().trajectoryID);
        }
        if (candidateNumberList != null) {
            candidateNumberList.add(candidateNumber);
            logger.info("candidate number: {}, scannedCandidateNumber: {}", candidateNumber, scannedCandidateNumber);
        }
        return resIDList;
    }

    public Map<Integer, Double> query(Trajectory trajectory) {
        if (this.rTree == null)
            throw new IllegalArgumentException("call buildRTree() first");
        Map<Integer, Double> idScores = new HashMap<>();
        List<GPXEntry> points = trajectory.getPoints();
        GPXEntry pre = points.get(0);
        double minLat = pre.getLat(), minLon = pre.getLon(), maxLat = pre.getLat(), maxLon = pre.getLon();
        double length = 0.0;
        double pointNumber = 1;
        for (int i = 1; i < points.size(); ++i) {
            double dist = GeoUtil.distance(pre, points.get(i));
            pre = points.get(i);
            length += dist;
            ++pointNumber;
            if (length >= MAX_LENGTH * 1.5) {
                rx.Observable<Entry<Integer, Geometry>> results = this.rTree.search(
                        Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat));
                calUpperBound(results, idScores, pointNumber);
                minLat = pre.getLat();
                minLon = pre.getLon();
                maxLat = pre.getLat();
                maxLon = pre.getLon();
                length = 0.0;
                pointNumber = 1;
                ++i;
            }
            if (pre.getLat() > maxLat) maxLat = pre.getLat();
            if (pre.getLat() < minLat) minLat = pre.getLat();
            if (pre.getLon() > maxLon) maxLon = pre.getLon();
            if (pre.getLon() < minLon) minLon = pre.getLon();
        }
        rx.Observable<Entry<Integer, Geometry>> results = this.rTree.search(
                Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat));
        calUpperBound(results, idScores, pointNumber);
        return idScores;
    }

    public List<Integer> rangeQuery(MMPoint point, double r) {
        double minLat = GeoUtil.increaseLatitude(point.getLat(), -r);
        double minLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -r);
        double maxLat = GeoUtil.increaseLatitude(point.getLat(), r);
        double maxLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), r);

        rx.Observable<Entry<Integer, Geometry>> results = this.rTree.search(
                Geometries.rectangleGeographic(minLon, minLat, maxLon, maxLat));

        Set<Integer> trajIDSet = new HashSet<>();
        results.forEach(entry -> {
            trajIDSet.add(entry.value());
        });
        return new ArrayList<>(trajIDSet);
    }

    /**
     * If the RTree dataStructure file exists, the subroutine will load them into RTree.
     * Otherwise it will buildTorGraph it using other data file on disk.
     *
     * @see #INDEX_PATH path to RTree dataStructure file
     * @see #buildRTree(Map) Build RTree from memory data.
     */
    public void buildRTree() {
        logger.info("Enter buildRTree");
        File indexFile = new File(INDEX_PATH);
        List<Envelope> validTrajEnvelopes = new ArrayList<>(1000000);
        if (!indexFile.exists()) {
            logger.info("dataStructure file doesn't exist");
            File envFile = new File(TRAJ_ENVELOPE_PATH);
            if (!envFile.exists()) {
                logger.info("envelope file doesn't exist");
                logger.info("get all trajectories");
                File trajIdFile = new File(TRAJ_ID_PATH);
                if (!trajIdFile.exists()) {
                    throw new IllegalArgumentException(TRAJ_ID_PATH + " doesn't exist");
                }
                Set<Integer> trajectoryIds = new HashSet<>();
                try {
                    Files.lines(Paths.get(TRAJ_ID_PATH)).forEach(line -> {
                        try {
                            trajectoryIds.add(Integer.parseInt(line));
                        } catch (NumberFormatException e) {
                            logger.error("{}, {}", line, e);
                        }
                    });
                } catch (IOException e) {
                    logger.error("{}", e);
                }
                List<Trajectory> trajectoryList = trajectoryService.getTrajectories(new ArrayList<>(trajectoryIds));
                logger.info("trajectoryList size: " + trajectoryList.size());
                logger.info("split trajectory and get envelope");
                Iterator<Trajectory> trajectoryIterator = trajectoryList.iterator();
                while (trajectoryIterator.hasNext()) {
                    Trajectory trajectory = trajectoryIterator.next();
                    List<GPXEntry> points = trajectory.getPoints();
                    GPXEntry pre = points.get(0);
                    double minLat = pre.getLat(), minLon = pre.getLon(), maxLat = pre.getLat(), maxLon = pre.getLon();
                    double length = 0.0;
                    boolean firstEnv = true;
                    for (int i = 1; i < points.size(); ++i) {
                        double dist = GeoUtil.distance(pre, points.get(i));
                        pre = points.get(i);
                        length += dist;
                        if (length >= MAX_LENGTH) {
                            if (firstEnv) {
                                validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                                        GeoUtil.increaseLongtitude(minLat, minLon, -GPS_ERROR_SIGMA),
                                        GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                                        maxLon,
                                        trajectory.getId()));
                            } else {
                                validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                                        minLon,
                                        GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                                        maxLon,
                                        trajectory.getId()));
                            }
                            minLat = pre.getLat();
                            minLon = pre.getLon();
                            maxLat = pre.getLat();
                            maxLon = pre.getLon();
                            length = 0.0;
                            ++i;
                        }
                        if (pre.getLat() > maxLat) maxLat = pre.getLat();
                        if (pre.getLat() < minLat) minLat = pre.getLat();
                        if (pre.getLon() > maxLon) maxLon = pre.getLon();
                        if (pre.getLon() < minLon) minLon = pre.getLon();
                    }
                    validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                            minLon,
                            GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                            GeoUtil.increaseLongtitude(maxLat, maxLon, GPS_ERROR_SIGMA),
                            trajectory.getId()));
                    trajectoryIterator.remove();
                }

                saveEnvelope(validTrajEnvelopes, envFile);
            } else {
                logger.info("envelope file exists");
                validTrajEnvelopes = readEnvelope(envFile);
            }

            logger.info("start to buildTorGraph R Tree dataStructure");
            this.rTree = RTree.star().maxChildren(6).create();
            Iterator<Envelope> envelopeIterator = validTrajEnvelopes.iterator();
            int rTreeProcess = 0;
            while (envelopeIterator.hasNext()) {
                if (rTreeProcess++ % 1000 == 0)
                    System.out.println(rTreeProcess);
                Envelope envelope = envelopeIterator.next();
                this.rTree = this.rTree.add(envelope.trajectoryId, Geometries.rectangleGeographic(envelope.minLon, envelope.minLat, envelope.maxLon, envelope.maxLat));
                envelopeIterator.remove();
            }
            logger.info("start to serialize dataStructure file to disk");
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            try (OutputStream os = new FileOutputStream(indexFile)) {
                serializer.write(this.rTree, os);
            } catch (Exception e) {
                logger.error("{}", e);
            }
        } else {
            logger.info("dataStructure file exists");
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            try (InputStream is = new FileInputStream(indexFile)) {
                this.rTree = serializer.read(is, indexFile.length(), InternalStructure.DEFAULT);
            } catch (FileNotFoundException e) {
                logger.error("{}", e);
            } catch (IOException e) {
                logger.error("{}", e);
            }
        }
        logger.info("Exit buildRTree");
    }

    /**
     * If the RTree dataStructure file exists, the subroutine will load them into RTree.
     * Otherwise it will buildTorGraph it using data passed to.
     * @param trajectoryMap key for trajectory, value for instances of type Trajectory.
     *                      note: the nodes in trajectoryMap should be calibrated points( points on virtual graph)
     * @see #buildRTree() Build RTree from file.
     */
    public void buildRTree(Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter buildRTree - {}", trajectoryMap.size());
        File indexFile = new File(INDEX_PATH);
        if (!indexFile.exists()) {
            logger.info("dataStructure file doesn't exist");

            //get all the envelops into validTrajEnvelopes
            List<Envelope> validTrajEnvelopes = new ArrayList<>(1000000);
            File envFile = new File(TRAJ_ENVELOPE_PATH);
            if (!envFile.exists()) {
                logger.info("envelope file doesn't exist");
                Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs))
                logger.info("trajectoryList size: " + trajectoryList.size());
                logger.info("split trajectory and get envelope");
                Iterator<Trajectory> trajectoryIterator = trajectoryList.iterator();
                while (trajectoryIterator.hasNext()) {
                    Trajectory trajectory = trajectoryIterator.next();
                    List<MMPoint> points = trajectory.getMMPoints();
                    MMPoint pre = points.get(0);
                    double minLat = pre.getLat(), minLon = pre.getLon(), maxLat = pre.getLat(), maxLon = pre.getLon();
                    int length = 0;
                    boolean firstEnv = true;
                    for (int i = 1; i < points.size(); ++i) {
                        pre = points.get(i);
                        length++;
                        if (length >= POINT_NUMBER_IN_MBR) {
                            if (firstEnv) {
                                validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                                                    GeoUtil.increaseLongtitude(minLat, minLon, -GPS_ERROR_SIGMA),
                                                    GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                                                    maxLon,
                                                    trajectory.getId()));
                            } else {
                                validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                                                    minLon,
                                                    GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                                                    maxLon,
                                                    trajectory.getId()));
                            }
                            minLat = pre.getLat();
                            minLon = pre.getLon();
                            maxLat = pre.getLat();
                            maxLon = pre.getLon();
                            length = 0;
                            ++i;
                        }
                        if (pre.getLat() > maxLat) maxLat = pre.getLat();
                        if (pre.getLat() < minLat) minLat = pre.getLat();
                        if (pre.getLon() > maxLon) maxLon = pre.getLon();
                        if (pre.getLon() < minLon) minLon = pre.getLon();
                        firstEnv = false;
                    }
                    validTrajEnvelopes.add(new Envelope(GeoUtil.increaseLatitude(minLat, -GPS_ERROR_SIGMA),
                            minLon,
                            GeoUtil.increaseLatitude(maxLat, GPS_ERROR_SIGMA),
                            GeoUtil.increaseLongtitude(maxLat, maxLon, GPS_ERROR_SIGMA),
                            trajectory.getId()));
                    trajectoryIterator.remove();
                }
                saveEnvelope(validTrajEnvelopes, envFile);
            } else {
                logger.info("envelope file exists");
                validTrajEnvelopes = readEnvelope(envFile);
            }

            logger.info("envelope size: {}", validTrajEnvelopes.size());
            logger.info("start to buildTorGraph R Tree dataStructure");
            this.rTree = RTree.star().maxChildren(6).create();
            Iterator<Envelope> envelopeIterator = validTrajEnvelopes.iterator();
            int counter = 0;
            trajectoryMBRMap = new HashMap<>();
            while (envelopeIterator.hasNext()) {
                if (counter++ % 100000 == 0)
                    logger.info("buildTorGraph R Tree dataStructure : {}", counter);
                Envelope envelope = envelopeIterator.next();
                Rectangle rectangle = Geometries.rectangleGeographic(envelope.minLon, envelope.minLat, envelope.maxLon, envelope.maxLat);
                List<Rectangle> mbr = trajectoryMBRMap.computeIfAbsent(envelope.trajectoryId, k -> new ArrayList<>());
                mbr.add(rectangle);
                this.rTree = this.rTree.add(envelope.trajectoryId, rectangle);
                //envelopeIterator.remove();
            }

            long noBytes = MemoryUtil.deepMemoryUsageOf(this.rTree, MemoryUtil.VisibilityFilter.ALL);
            logger.info("bytes of RTree is: {}", noBytes);
            logger.info("start to serialize dataStructure file to disk, big file cannot be serilized");
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            try (OutputStream os = new FileOutputStream(indexFile)) {
                serializer.write(this.rTree, os);
            } catch (FileNotFoundException e) {
                logger.error("{}", e);
            } catch (IOException e) {
                logger.error("{}", e);
            } catch (Exception e) {
                logger.error("{}", e);
            }
        } else {
            logger.info("dataStructure file exists");
            Serializer<Integer, Geometry> serializer = Serializers.flatBuffers().javaIo();
            try (InputStream is = new FileInputStream(indexFile)) {
                this.rTree = serializer.read(is, indexFile.length(), InternalStructure.DEFAULT);
            } catch (FileNotFoundException e) {
                logger.error("{}", e);
            } catch (IOException e) {
                logger.error("{}", e);
            }
        }
        logger.info("Exit buildRTree");
    }

    private List<Envelope> readEnvelope(File file) {
        List<Envelope> validTrajEnvelopes = new LinkedList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Envelope envelope = new Envelope(line);
                validTrajEnvelopes.add(envelope);
            }
            logger.info("validTrajEnvelopes size: {}", validTrajEnvelopes.size());
        } catch (IOException e) {
            logger.error("{}", e);
        }
        return validTrajEnvelopes;
    }

    class Pair {
        public final int trajectoryID;
        public double lowerBound;

        Pair(int trajectoryID, double lowerBound) {
            this.lowerBound = lowerBound;
            this.trajectoryID = trajectoryID;
        }
    }

    /**
     * minimum bounding box for a trajectory
     */
    class Envelope {
        public double minLat, minLon, maxLat, maxLon;
        public int trajectoryId;

        private final String SEPARATOR = " ";

        public Envelope() {
        }

        public Envelope(double minLat, double minLon, double maxLat, double maxLon, int trajectoryId) {
            this.minLat = minLat;
            this.minLon = minLon;
            this.maxLat = maxLat;
            this.maxLon = maxLon;
            this.trajectoryId = trajectoryId;
        }

        public Envelope(String line) {
            String[] array = line.split(SEPARATOR);
            this.minLat = Double.parseDouble(array[0]);
            this.minLon = Double.parseDouble(array[1]);
            this.maxLat = Double.parseDouble(array[2]);
            this.maxLon = Double.parseDouble(array[3]);
            this.trajectoryId = Integer.parseInt(array[4]);
        }

        @Override
        public String toString() {
            return minLat +
                    SEPARATOR + minLon +
                    SEPARATOR + maxLat +
                    SEPARATOR + maxLon +
                    SEPARATOR + trajectoryId;
        }
    }
}

