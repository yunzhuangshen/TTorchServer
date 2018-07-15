package au.edu.rmit.trajectory.torch.io;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.EfficiencyExp;
import au.edu.rmit.trajectory.similarity.algorithm.FastTimeSeriesEvaluation;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.algorithm.Transformation;
import au.edu.rmit.trajectory.similarity.datastructure.NodeGridIndex;
import au.edu.rmit.trajectory.similarity.datastructure.EdgeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.NodeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.TraGridIndex;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.datastructure.RTreeWrapper;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import au.edu.rmit.trajectory.similarity.util.CommonUtils;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.storage.Graph;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The class contains method for processing and indexing Beijing dataset
 *
 * @author forrest0402
 * @Description
 * @date 1/25/2018
 */
@Component
public class BeijingDataset {

    private static Logger logger = LoggerFactory.getLogger(BeijingDataset.class);

    /**
     * wrapper for graphHopper API
     ***************************/
    final
    Mapper mapper;

    /**
     * utils for dealing with file related issues
     ***************************/
    final
    FileManager fileManager;

    /**
     * for retrieve project resources under spring framework
     ***************************/
    final
    Environment environment;

    /**
     * contains methods to test efficiency of varies algorithms under different indexes
     ***************************/
    final
    EfficiencyExp efficiencyExp;

    /**
     * Raw trajectory data file
     ***************************/
    private final static String RAW_FILE_PATH = "taxi_log_2008_by_id";

    /**
     * All points on virtual graph, preprocess from beijing.osm.pbf
     ***************************/
    private final static String BEIJING_POINT_FILE = "BEIJING DATA/BEIJING_POINT_FILE";

    /**
     * All edges on virtual graph, preprocess from beijing.osm.pbf
     ***************************/
    private final static String BEIJING_EDGE_FILE = "BEIJING DATA/BEIJING_EDGE_FILE";

    /**
     * Split trajectory parameter
     ***************************/
    private final static double DISTANCE_THREASHOLD = 5000;

    /**
     * after map matching of RAW_TRAJ_BEIJING
     ***************************/
    private final static String BEIJING_FILE = "BEIJING DATA/BEIJING_DATASET";

    /**
     * after preprocessing(combining, splitting etc) of RAW_FILE_PATH
     ***************************/
    private final static String SPLIT_RAW_BEIJING_FILE = "BEIJING DATA/RAW_TRAJ_BEIJING";

    /**
     * Query file
     ***************************/
    private final static String EFFICIENCY_QUERY_FILE = "BEIJING DATA/EFFICIENCY_QUERY_FILE";

    /**
     * Index
     ***************************/
    private final NodeInvertedIndex nodeInvertedIndex;

    private final EdgeInvertedIndex edgeInvertedIndex;

    private final TraGridIndex gridIndex;

    private final RTreeWrapper envelopeIndex;

    private final NodeGridIndex nodeGridIndex;

    private final Transformation transformation;

    private final FastTimeSeriesEvaluation FTSE;

    @Autowired
    public BeijingDataset(FileManager fileManager, Mapper mapper, Environment environment, EfficiencyExp efficiencyExp, NodeInvertedIndex nodeInvertedIndex, EdgeInvertedIndex edgeInvertedIndex, TraGridIndex gridIndex, RTreeWrapper envelopeIndex, NodeGridIndex nodeGridIndex, Transformation transformation, FastTimeSeriesEvaluation FTSE) {
        this.fileManager = fileManager;
        this.mapper = mapper;
        this.environment = environment;
        this.efficiencyExp = efficiencyExp;
        this.nodeInvertedIndex = nodeInvertedIndex;
        this.edgeInvertedIndex = edgeInvertedIndex;
        this.gridIndex = gridIndex;
        this.envelopeIndex = envelopeIndex;
        this.nodeGridIndex = nodeGridIndex;
        this.transformation = transformation;
        this.FTSE = FTSE;
    }

    /**
     *
     * @param measureType enum where has instances LORS, LCSS, DTW, etc
     * @param calibrated if true, use mapped osm points for both query and trajectory
     *                   if false, use points on trajectory to do the search
     * @param k threshold for specifying number of results
     */
    public void FindTopK(MeasureType measureType, boolean calibrated, int k) {
        logger.info("Enter FindTopK - {} {}", measureType, calibrated);
        Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(calibrated);
        List<Trajectory> queryList = loadQueryMap(calibrated).get(2);
        AtomicInteger process = new AtomicInteger(0);
        int numberOfCpuCores = CommonUtils.getNumberOfCPUCores() + 1;
        logger.info("cpu cores: {}", numberOfCpuCores);
        ExecutorService threadPool = new ThreadPoolExecutor(numberOfCpuCores * 2, numberOfCpuCores * 4, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        ReentrantLock reentrantLock = new ReentrantLock();
        String fileName = calibrated ? "mm.top" + k + "." + measureType.toString() : "top" + k + "." + measureType.toString();
        deleteFile(fileName);
        for (Trajectory query : queryList) {
            threadPool.execute(() -> {
                if (process.getAndIncrement() % 10 == 0)
                    logger.info("process: {}", process.intValue());
                PriorityQueue<Pair> topKHeap = null;
                double bestSoFar;
                if (measureType == MeasureType.LCSS || measureType == MeasureType.LORS) {
                    topKHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p1.score, p2.score));
                    bestSoFar = -Double.MAX_VALUE;
                } else if (measureType == MeasureType.EDR || measureType == MeasureType.DTW) {
                    topKHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p2.score, p1.score));
                    bestSoFar = Double.MAX_VALUE;
                } else throw new IllegalArgumentException("cannot recongize measureType");
                for (Trajectory candidate : trajectoryMap.values()) {
                    double score;
                    switch (measureType) {
                        case LCSS:
                            score = similarityMeasure.LongestCommonSubsequence(candidate.getMMPoints(), query.getMMPoints(), 200);
                            break;
                        case EDR:
                            score = similarityMeasure.EditDistanceonRealSequence(candidate.getMMPoints(), query.getMMPoints());
                            break;
                        case DTW:
                            if (Math.abs(candidate.getMMPoints().size() - query.getMMPoints().size()) < 200)
                                score = similarityMeasure.DynamicTimeWarping(candidate.getMMPoints(), query.getMMPoints());
                            else score = Double.MAX_VALUE;
                            break;
                        case LORS:
                            score = similarityMeasure.LongestCommonSubsequence(candidate.getMMPoints(), query.getMMPoints(), Integer.MAX_VALUE);
                            break;
                        default:
                            score = 0;
                    }
                    topKHeap.add(new Pair(candidate.getId(), score));
                    if (topKHeap.size() > k) {
                        topKHeap.poll();
                        bestSoFar = topKHeap.peek().score;
                    }
                }
                try {
                    reentrantLock.lock();
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
                        writer.write(query.getId() + "");
                        while (topKHeap.size() > 0) {
                            Pair pair = topKHeap.poll();
                            writer.write(" " + pair.key + "," + pair.score);
                        }
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } finally {
                    reentrantLock.unlock();
                }
            });
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("Exit FindTopK - {} {}", measureType, calibrated);
    }

    /**
     * generated binary files:
     * doc file:  first int is "1"
     *            second int is number of trajectories in total
     *            from third int, it indicates rest number of ints representing trajectory hash that associate with the same point
     *            e.g. the third int is 10. it means the next 10 int representing tra_id associate with the same point
     * freq file: the first int indicates rest number of ints that associate with the point
     *            e.g. the first int is 10. it means next 10 int representing tra_id associate with the same point
     * size file: first int is number of trajectories in total,
     *            from second int, it indicates number of 'calibrated' nodes on trajectories
     *
     * note: 1. points on trajectory is not raw point but 'calibrated' points
     *       2. the dataStructure it creates is not point inverted dataStructure, as we cannot find trajectories that has the point on it.
     * @see #nodeInvertedIndex for note2.
     *
     *
     * @param toggleOff indicate if block should be executed.
     */
    private void createInvertedIndex(boolean toggleOff) {
        if (toggleOff) return;
        //key for point hash, value for trajectory hash and frequency
        Map<Integer, Map<Integer, Integer>> invertedIndex = new HashMap<>();
        Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(true);
        Map<Integer, Integer> allPointMap = new HashMap<>();
        logger.info("trajectory size: {}", trajectoryMap.size());
        logger.info("reordering point id");

        // assign ids for points on virtual map. (it will take hashcode as hash by default)
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        int idSeq = 0;
        for (Trajectory trajectory : trajectoryList) {
            for (MMPoint point : trajectory.getMMPoints()) {
                if (!allPointMap.containsKey(point.getId())) {
                    allPointMap.put(point.getId(), idSeq++);
                }
            }
        }

        logger.info("buildTorGraph invert dataStructure");
        // for each point, record hash of trajectory across it, and number of times the trajectory across it.
        // if one trajectory have 2 same point on it, its frequency should be 2.
        for (Trajectory trajectory : trajectoryList) {
            for (MMPoint point : trajectory.getMMPoints()) {
                try {
                    int pointID = allPointMap.get(point.getId());
                    Map<Integer, Integer> postingMap = invertedIndex.computeIfAbsent(pointID, k -> new HashMap<>());
                    postingMap.merge(trajectory.getId(), 1, (a, b) -> a + b);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        logger.info("write to file");

        try (OutputStream docWriter = new FileOutputStream("beijing.docs");
             OutputStream freqWriter = new FileOutputStream("beijing.freqs");
             OutputStream sizeWriter = new FileOutputStream("beijing.sizes")) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            //docs and freqs
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            writeInt(1, buffer, docWriter);
            writeInt(trajectoryList.size(), buffer, docWriter);

            for (Map.Entry<Integer, Map<Integer, Integer>> entry : invertedIndex.entrySet()) {
//                int pointID = entry.getKey();
//                Map<Integer, Integer> postingList = invertedIndex.get(pointID);
                Map<Integer, Integer> postingList = entry.getValue();
                writeInt(postingList.size(), buffer, docWriter);
                writeInt(postingList.size(), buffer, freqWriter);
                for (Map.Entry<Integer, Integer> posting : postingList.entrySet()) {
                    writeInt(posting.getKey(), buffer, docWriter);
                    writeInt(posting.getValue(), buffer, freqWriter);
                }
                docWriter.flush();
                freqWriter.flush();
            }
            //size
            writeInt(trajectoryList.size(), buffer, sizeWriter);
            for (Trajectory trajectory : trajectoryList) {
                writeInt(trajectory.getMMPoints().size(), buffer, sizeWriter);
            }
            sizeWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * find all trajectories at least having one edge in common with the query trajectory. Save to disk
     *
     * output file: intersect.idx: each line contains 1 query trajectory hash and a list of ids that are trajectories relevant to it.
     *
     */
    public void createIntersectIndex(boolean toggleOff) {
        if (toggleOff) return;
        logger.info("Enter createIntersectIndex");
        Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(true);
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);

        if (!edgeInvertedIndex.load()) {
            Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
            edgeInvertedIndex.buildIndex(trajectoryList, allEdgeMap);
        }

        //given a trajectory as query, find all relevant( at least contains one same edge as queryTrajectory ) trajectories
        //key is trajectory hash, value is relevant trajectory hash list
        Map<Integer, Set<Integer>> results = new ConcurrentHashMap<>();
        ExecutorService threadPool = new ThreadPoolExecutor(40, 40, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0);
        List<Trajectory> queryList = loadQueryMap(true).get(2);
        ReentrantLock reentrantLock = new ReentrantLock();
        logger.info("start to find - {}", queryList.size());
        for (Trajectory trajectory : queryList) {
            threadPool.execute(() -> {
                process.incrementAndGet();
                System.out.println("process: " + process.intValue());
                List<MMEdge> originalSegments = trajectory.getMapMatchedTrajectory(allEdgeMap);
                for (MMEdge edge : originalSegments) {
                    List<Integer> relevantTrajList = edgeInvertedIndex.findRelevantTrajectoryID(edge.getId());
                    try {
                        reentrantLock.lock();
                        Set<Integer> relevantIDSet = results.computeIfAbsent(trajectory.getId(), k -> new HashSet<>());
                        relevantIDSet.addAll(relevantTrajList);
                    } finally {
                        reentrantLock.unlock();
                    }
                }
            });
        }
        logger.info("start to shutdown");
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("start to write files");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter("intersect.idx"))) {
            for (Map.Entry<Integer, Set<Integer>> entry : results.entrySet()) {
                bw.write(entry.getKey() + "");
                for (Integer integer : entry.getValue()) {
                    bw.write(" " + integer);
                }
                bw.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit createIntersectIndex");
    }

    /**
     * for purpose of observe statistics of trajectory
     *
     * @param toggleOff indicate if block should be executed.
     */
    private void getDatasetInfo(boolean toggleOff) {
        if (toggleOff) return;

        //each trajectory is defined by original gps coordinate.
        Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(false);
        logger.info("trajectory size: {}", trajectoryMap.size());
        double[] arguments = new double[10];
        arguments[3] = Double.MAX_VALUE;
        ReentrantLock reentrantLock = new ReentrantLock();
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        trajectoryList.stream().parallel().forEach(trajectory -> {
            double dist = trajectory.getTotalLength();
            try {
                reentrantLock.lock();
                arguments[0] += trajectory.getMMPoints().size();             // sum of size of each trajectory
                arguments[1] += dist;                                        // sum of distance of each trajectory
                if (dist > arguments[2]) arguments[2] = dist;                // max distance in trajectory set
                if (dist < arguments[3]) arguments[3] = dist;                // min distance in trajectory set
                arguments[4] += dist / (trajectory.getMMPoints().size() - 1);// sum of average distance of edges for each trajectory
            } finally {
                reentrantLock.unlock();
            }
        });

        logger.info("Beijing dataset sampling rate is {} between points.", arguments[4] / trajectoryMap.size());
        logger.info("min trajectory length: {}", arguments[3]);
        logger.info("max trajectory length: {}", arguments[2]);
        logger.info("avg trajectory length: {}", arguments[1] / (trajectoryMap.size()));
        //logger.info("trajectory point number: {}", arguments[0]);
        logger.info("trajectory raw point number in avg: {}", arguments[0] / trajectoryMap.size());

        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        logger.info("Beijing graph points in total: {}", allPointMap.size());
        logger.info("Beijing graph edges in total: {}", allEdgeMap.size());
        double edgelength = 0, edgeMinLength = Double.MAX_VALUE, edgeMaxLength = 0;
        for (MMEdge edge : allEdgeMap.values()) {
            edgelength += edge.getLength();
            if (edge.getLength() > edgeMaxLength) edgeMaxLength = edge.getLength();
            if (edge.getLength() < edgeMinLength) edgeMinLength = edge.getLength();
        }
        logger.info("Beijing graph edge min length: {}", edgeMinLength);
        logger.info("Beijing graph edge max length: {}", edgeMaxLength);
        logger.info("Beijing graph edge length in avg: {}", edgelength / allEdgeMap.size());

        //each trajectory is defined by mapped points on virtual graph from gps coordinate.
        Map<Integer, Trajectory> trajectoryMap2 = loadBeijingTrajectoryTxtFile(true);
        OptionalDouble segmentLength = trajectoryMap2.values().stream().parallel().mapToDouble(trajectory -> {
            double tra_dist = 0;
            List<MMEdge> mappedEdges = trajectory.getMapMatchedTrajectory(allEdgeMap);
            for (MMEdge originalSegment : mappedEdges) {
                tra_dist += originalSegment.getLength();
            }
            return tra_dist / mappedEdges.size();
        }).average();
        logger.info("All Beijing graph edges that could be mapped to trajectories length in avg: {}", segmentLength.getAsDouble() / trajectoryMap2.size());

    }

    /**
     *
     * compression rationale: VByte
     * example: if we are about to compress Integer 11 (00000000 00000000 00000000 00001011) into byte, we only need 1 byte.
     *
     * @param fileName  path of output file
     * @param allPointMap key: trajectory hash
     *                    value: a list of instances of type TorVertex meaning the points on that trajectory
     */
    public void compressTrajectoryFile(String fileName, Map<Integer, MMPoint> allPointMap) {
        logger.info("Enter compressTrajectoryFile");
        byte[] SEPBYTE = new byte[]{0x1F, 0x3F, 0x5F, 0x7F};
        ByteBuffer buffer = ByteBuffer.allocate(4);

        //docs and freqs
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteIntegerCODEC bic = new VariableByte();
        int idSeq = 0;
        Map<Integer, Integer> idReorderMap = new HashMap<>();
        for (MMPoint point : allPointMap.values()) {
            if (!idReorderMap.containsKey(point.getId())) {
                idReorderMap.put(point.getId(), idSeq++);
            }
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
             OutputStream posBufWriter = new FileOutputStream(fileName + ".compress", false)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] array = line.split("\t");
                String[] idArray = array[1].split(",");
                int[] data = new int[idArray.length];
                for (int i = 0; i < idArray.length; ++i)
                    data[i] = Integer.parseInt(idArray[i]);
                for (int i = 0; i < data.length; i++) {
                    data[i] = idReorderMap.get(data[i]);
                }
                byte[] outArray = new byte[1000000];
                writeInt(Integer.parseInt(array[0]), buffer, posBufWriter);
                IntWrapper inPos = new IntWrapper(), outPos = new IntWrapper();
                bic.compress(data, inPos, data.length, outArray, outPos);
                posBufWriter.write(outArray, 0, outPos.get());
                posBufWriter.write(SEPBYTE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit compressTrajectoryFile");
    }

    @PostConstruct
    public void start() {
        logger.info("Enter start");
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);

        //raw data to refined trajectory data with mapping edges
        //RawData2Trajectory(false);
        preprocessingBeijingTrajectoryData(false);

        //generate queries
        generateEfficiencyIDs(BEIJING_FILE, false);
        convertQueriesToTenForms(true, false);

        //for data observation purpose
        test(true);

        createInvertedIndex(true);
        createIntersectIndex(true);

        //for data observation purpose
        getDatasetInfo(true);

        int k = 150;
        //FindTopK(MeasureType.LCSS, true, k);
        //FindTopK(MeasureType.LCSS, false, k);
        //FindTopK(MeasureType.EDR, true, k);
        //FindTopK(MeasureType.EDR, false, k);
        //FindTopK(MeasureType.DTW, true, k);
        //FindTopK(MeasureType.DTW, false, k);
        //FindTopK(MeasureType.LORS, false, k);
        if (false) {
            loadEdgeIndex(edgeInvertedIndex, false);
            efficiencyExp.efficiencyOfLORS(false, edgeInvertedIndex, loadQueryMap(true), allEdgeMap);
        }
        if (false) {
            edgeInvertedIndex.load();
            edgeInvertedIndex.loadCompressedForm();
            efficiencyExp.efficiencyOfLORSWithCompression(false, edgeInvertedIndex, loadQueryMap(true), allEdgeMap);
        }
        if (false) {
            efficiencyExp.efficiencyOfFTSE(false, FTSE, loadQueryMap(false), loadBeijingTrajectoryTxtFile(false));
        }
        if (false) {
            efficiencyExp.efficiencyOfLEVI(false, nodeInvertedIndex, nodeGridIndex, loadQueryMap(true), allPointMap);
        }
        if (false) {
            nodeInvertedIndex.loadCompressedForm();
            efficiencyExp.efficiencyOfLEVIWithCompression(false, nodeInvertedIndex, nodeGridIndex, loadQueryMap(true), allPointMap);
        }
        if (false) {
            efficiencyExp.efficiencyOfDTWBasedOnRTree(envelopeIndex, loadBeijingTrajectoryTxtFile(false), loadQueryMap(false));
        }
        if (false) {
            efficiencyExp.efficiencyOfDTWOnGridIndex(nodeGridIndex, loadBeijingTrajectoryTxtFile(true), loadQueryMap(true), allEdgeMap, allPointMap);
        }
        if (false) {
            efficiencyExp.efficiencyOfRTreeBasedRangeQuery(envelopeIndex, loadBeijingTrajectoryTxtFile(false));
        }
        if (false) {
            efficiencyExp.efficiencyOfGridBasedRangeQuery(nodeGridIndex, nodeInvertedIndex, allPointMap);
        }
        if (false) {
            efficiencyExp.efficiencyOfPathQuery(edgeInvertedIndex, loadQueryMap(true), allEdgeMap);
        }
        if (false) {
            efficiencyExp.efficiencyOfStrictPathQuery(edgeInvertedIndex, loadQueryMap(true), allEdgeMap);
        }
        logger.info("Exit start");
    }

    /**
     * flow:
     * - load .osm.pbf data and generate virtual graph with GraphHopper API
     * - model all points and edges on the virtual graph and cache them into lists.
     * - do mapmatching with GraphHopper API for each trajectory.
     * - store a string of edges hash into each trajectory.
     * - save edges, points, trajectories on disk.
     *
     * input data file:  BEIJING DATA/RAW_TRAJ_BEIJING, source to generate virtual graph
     * output file:  - target/beijingmapmatchingtest, graph data generated by GraphHopper
     *               - BEIJING DATA/BEIJING_DATASET, compared with BEIJING DATA/RAW_TRAJ_BEIJING, it has edge ids mapping to the trajectory with each one
     *               - BEIJING DATA/
     *
     * @param force indicate whether generate in the case of the file about to generate exists
     *              if true, we are going to regenerate it no matter the file exists or not.
     *              if false and the file exists, we won't do it a second time.
     */
    public void preprocessingBeijingTrajectoryData(boolean force) {
        logger.info("Enter preprocessingBeijingTrajectoryData");
        File file = new File(BEIJING_FILE);
        if (file.exists() && !force) {
            logger.info("Exit - file exist");
            return;
        } else if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }

        // load all trajectories from BEIJING DATA/RAW_TRAJ_BEIJING into trajectoryMap
        Map<Integer, Trajectory> trajectoryMap = loadTrajectoryTxtFile(SPLIT_RAW_BEIJING_FILE, true, 1000000);
        // The map will be filled with points formed from osm data
        Map<Integer, MMPoint> allPoints = new HashMap<>();
        // The map will be filled with edges formed from osm data
        Map<Integer, MMEdge> allEdges = new HashMap<>();
        // The subroutine generate a virtual graph( instance of Graph Class) from osm dataset.
        // by utilizing API provided by GraphHopper
        Graph graph = mapper.getGraph(environment.getProperty("BEIJING_PBF_FILE_PATH"), "./target/beijingmapmatchingtest");
        GraphHopper hopper = mapper.getHopper();
        logger.info("node size: {}", graph.getNodes());

        // model all the points on the graph.
        for (int i = 0; i < graph.getNodes(); ++i) {
            //graph.getNodeAccess().getLat(i) return the latitude of ith node
            //graph.getNodeAccess().getLon(i) return the latitude of ith node
            MMPoint towerPoint = new MMPoint(graph.getNodeAccess().getLat(i), graph.getNodeAccess().getLon(i), true);
            allPoints.put(towerPoint.hashCode(), towerPoint);
        }

        logger.info("node size: {}", allPoints.size());
        // preprocess all edges
        logger.info("edge size: {}", graph.getAllEdges().getMaxId());

        AllEdgesIterator allEdgeIterator = graph.getAllEdges();
        boolean[] visit = new boolean[graph.getAllEdges().getMaxId() + 1];

        // model all edges on the graph
        while (allEdgeIterator.next()) {
            MMEdge edge = new MMEdge(allEdgeIterator, hopper);
            visit[allEdgeIterator.getEdge()] = true;
            edge.convertToDatabaseForm();
            if (!allEdges.containsKey(edge.getId())) {
                allEdges.put(edge.getId(), edge);
            }
        }
        logger.info("edge size: {}", allEdges.size());

        initBeijingTrajectoryMapping();

        logger.info("start map matching");
        ExecutorService threadPool = new ThreadPoolExecutor(10, 15, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0),
                      failedNum = new AtomicInteger(0);

        // find matched edges for each trajectory
        for (Trajectory trajectory : trajectoryMap.values()) {
            threadPool.execute(() -> {
                try {
                    if (process.getAndIncrement() % 100 == 0) {
                        System.out.println(failedNum.intValue() + "/" + process.intValue());
                    }
                    List<MMEdge> rawEdges = mapper.match(trajectory.getPoints());
                    StringBuilder edgeStr = new StringBuilder(rawEdges.size());
                    synchronized (BeijingDataset.class) {
                        for (MMEdge rawEdge : rawEdges) {
                            MMEdge edge = allEdges.get(rawEdge.getId());
                            if (edge == null) {
                                rawEdge.convertToDatabaseForm();
                                allEdges.put(rawEdge.getId(), rawEdge);
                            }
                            if (edgeStr.length() > 0)
                                edgeStr.append(Common.instance.SEPARATOR);
                            edgeStr.append(rawEdge.getId());
                        }
                    }
                    trajectory.setEdgeStr(edgeStr.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                    failedNum.getAndIncrement();
                }
            });
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("save all edges and points and matched trajectories");
        Dataset.storeGraph(BEIJING_POINT_FILE, BEIJING_EDGE_FILE, allPoints.values(), allEdges.values());
        try (BufferedWriter trajWriter = new BufferedWriter(new FileWriter(BEIJING_FILE, false))) {
            for (Trajectory trajectory : trajectoryMap.values()) {
                trajWriter.write(trajectory.toString());
                trajWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit preprocessingBeijingTrajectoryData");
    }

    /**
     *
     * Generate query
     * A query is essentially a trajectory. The subroutine finds qualified trajectories.
     * qualified trajectories:
     * - amount of node on it is larger than 100;
     * - real distance is less than 100000
     * - its subtrajectories ( 20,40, 60, 80 nodes) should be map-able on virtual map
     * - save those qualified ones to file
     *
     * input file: BEIJING DATA/BEIJING_DATASET
     * output file: BEIJING DATA/EFFICIENCY_QUERY_FILE
     *
     * @param trajectoryFile the URI to the input file
     * @param force indicate whether generate in the case of the file about to generate exists
     *              if true, we are going to regenerate it no matter the file exists or not.
     *              if false and the file exists, we won't do it a second time.
     *
     */
    public void generateEfficiencyIDs(String trajectoryFile, boolean force) {
        logger.info("Enter generateEfficiencyIDs");
        File file = new File(EFFICIENCY_QUERY_FILE);
        if (file.exists() && !force) {
            logger.info("Exit - file exits");
            return;
        } else if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }

        Map<Integer, Trajectory> trajectoryMap = loadTrajectoryTxtFile(trajectoryFile, false, 10000000);
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        double length = 0;
        for (Trajectory trajectory : trajectoryList) {
            length += trajectory.getPoints().size();
        }
        logger.info("average length is {}", length / trajectoryList.size());

        //select trajectories whose point size is larger than 100 and geolength is less than 100000 from all
        List<Integer> idList = new ArrayList<>();
        for (Trajectory trajectory : trajectoryList) {
            if (trajectory.getPoints().size() >= 100 && trajectory.getTotalLength() < 100000)
                idList.add(trajectory.getId());
        }
        logger.info("valid trajectory size: {}", idList.size());

        //randomly pick 150 queries with length of 100
        SecureRandom random = new SecureRandom();
        Set<Integer> luckyIDSet = new HashSet<>();
        Set<Integer> invalidCandidates = new HashSet<>();
        initBeijingTrajectoryMapping();

        while (luckyIDSet.size() + invalidCandidates.size() < idList.size() && luckyIDSet.size() < 300) {
            int trajectoryID = idList.get(random.nextInt(idList.size()));
            if (invalidCandidates.contains(trajectoryID)) continue;
            if (luckyIDSet.size() % 10 == 0)
                logger.info("valid size: {}, invalid size: {}, total size: {}", luckyIDSet.size(), invalidCandidates.size(), idList.size());

            //I have to make sure all sub sample trajectories can be map matched
            //otherwise it can not be used. Add those to invalid list
            List<MMPoint> points = new ArrayList<>();
            Trajectory candidate = trajectoryMap.get(trajectoryID);
            try {
                for (int i = 0; i < 100; ++i) {
                    points.add(candidate.getMMPoints().get(i));
                    if (points.size() % 20 == 0) {
                        List<MMPoint> pathPoints = new ArrayList<>();
                        List<MMEdge> edges = new ArrayList<>();
                        mapper.match(points, pathPoints, edges);
                    }
                }
                luckyIDSet.add(trajectoryID);
            } catch (Exception e) {
                //todo question: if the first 60 points is ok, but when it comes to 80 points, match() throws an exception. Shouldn't we remove it from luckyIDSet?
                //luckyIDSet.remove(trajectoryID);
                invalidCandidates.add(trajectoryID);
            }
        }
        logger.info("luckyIDSet size: {}", luckyIDSet.size());

        //save files
        List<Integer> sortedIDList = new ArrayList<>(luckyIDSet);
        sortedIDList.sort(Comparator.naturalOrder());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(EFFICIENCY_QUERY_FILE, false))) {
            for (Integer id : sortedIDList) {
                Trajectory trajectory = trajectoryMap.get(id);
                writer.write(trajectory.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit generateEfficiencyIDs");
    }

    /**
     * Load trajectory data to map from file.
     * instance of Trajectory Type will be constructed and initiated with hash, points, edges.
     *
     * @param trajectoryFile path to the file representing by String object.
     * @param loadNullEdge it determines if load the record( trajectory) that the third field(edgeStr) is empty
     * @param limit it determines amount of trajectories the program is going to load to map
     * @return res a map which contains key(trajectoryId)-value(Trajectory instance)
     */
    public static Map<Integer, Trajectory> loadTrajectoryTxtFile(String trajectoryFile, boolean loadNullEdge, int limit) {
        logger.info("Enter loadTrajectoryTxtFile - start to preprocess trajecties from {}", trajectoryFile);
        Map<Integer, Trajectory> res = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(trajectoryFile));
            String _line = null;
            AtomicInteger process = new AtomicInteger(0);

            ExecutorService threadPool = new ThreadPoolExecutor(10, 15, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            while ((_line = reader.readLine()) != null) {
                final String line = _line;
                threadPool.execute(() -> {
                    final String filteredLineStr = line.replace("\"", "");
                    if (limit == process.intValue()) return;
                    if (process.incrementAndGet() % 50000 == 0)
                        logger.info("Reading {} - {}", process.intValue(), trajectoryFile);
                    String[] array = filteredLineStr.split("\t");
                    int id = Integer.parseInt(array[0]);
                    String edgeStr = array.length == 3 ? array[2] : null;
                    Trajectory trajectory = new Trajectory(id, array[1], edgeStr);
                    trajectory.getMMPoints();
                    synchronized (BeijingDataset.class) {
                        if (loadNullEdge)
                            res.put(id, trajectory);
                        else {
                            if (edgeStr != null) res.put(id, trajectory);
                        }
                    }
                });
            }
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.error("{}", e);
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
        logger.info("Exit loadTrajectoryTxtFile - {}", res.size());
        return res;
    }

    /**
     * For data observation purpose.
     *
     * Choose 1 query( a sub node set selected from origin trajectory set) represented in two node set.
     * One of them is represented by origin point on trajectory, and the other( calibrated) is represented by mapped point on virtual graph.
     * compute and compare using different distance function calibrated query with one trajectory( also got calibrated).
     *
     * @param toggleOff indicate if execute this block.
     */
    public void test(boolean toggleOff) {
        if (toggleOff) return;
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        Map<Integer, List<Trajectory>> queryMap2 = loadQueryMap(false);
        Trajectory query = null, query2 = null;
        for (Trajectory trajectory : queryMap.get(2)) {
            if (trajectory.getId() == 33303) {
                query = trajectory;
                break;
            }
        }
        for (Trajectory trajectory : queryMap2.get(2)) {
            if (trajectory.getId() == 33303) {
                query2 = trajectory;
                break;
            }
        }

        Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(true);
        Trajectory candidate = trajectoryMap.get(53339);
        double score = similarityMeasure.fastDynamicTimeWarping(candidate.getMMPoints(), query.getMMPoints(), 100, 0, null);
        double score2 = similarityMeasure.DynamicTimeWarping(candidate.getMMPoints(), query.getMMPoints());
        System.out.println(score + ", " + score2);
        Transformation.printPoints(query2.getMMPoints());
        Transformation.printPoints(query.getMMPoints());
        Transformation.printPoints(candidate.getMMPoints());
        System.out.println();
    }

    /**
     * Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(true);
     * after calibrate(), getMMPoints() can get calibrated points
     *
     * @param calibrated
     * @return
     */
    public static Map<Integer, Trajectory> loadBeijingTrajectoryTxtFile(boolean calibrated) {
        logger.info("Enter loadBeijingTrajectoryTxtFile");
        Map<Integer, Trajectory> trajectoryMap = loadTrajectoryTxtFile(BEIJING_FILE, true, Integer.MAX_VALUE);
        if (calibrated) {
            logger.info("start to calibrate trajectories");
            Collection<Trajectory> trajectoryList = trajectoryMap.values();
            Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
            Map<Integer, MMPoint> allPointMap = new HashMap<>();
            getGraphMap(allEdgeMap, allPointMap);
            //Iterator<Trajectory> iter = trajectoryList.iterator();
            trajectoryList.stream().parallel().forEach(trajectory -> {
                trajectory.getMapMatchedTrajectory(allEdgeMap);
                trajectory.calibrate();
            });
        }
        logger.info("Exit loadBeijingTrajectoryTxtFile");
        return trajectoryMap;
    }

    /**
     * load edges and its inverted list containing its corresponding trajectories
     *
     * @param edgeInvertedIndex the intance of type EdgeIndex to be loaded to
     * @param compress it determines which dataStructure file would be used.
     *                 if true, the data will be loaded and constructed from compressed version of edge dataStructure files.
     *                 if false, the data will be loaded and constructed from non-compressed version of edge dataStructure files.
     */
    public static void loadEdgeIndex(EdgeInvertedIndex edgeInvertedIndex, boolean compress) {
        if (compress) {
            edgeInvertedIndex.loadCompressedForm();
        } else {
            if (!edgeInvertedIndex.load()) {
                Map<Integer, Trajectory> trajectoryMap = loadBeijingTrajectoryTxtFile(true);
                Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
                Map<Integer, MMPoint> allPointMap = new HashMap<>();
                getGraphMap(allEdgeMap, allPointMap);
                Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
                edgeInvertedIndex.buildIndex(trajectoryList, allEdgeMap);
            }
        }
    }

    private void initBeijingTrajectoryMapping() {
        mapper.clear();
        mapper.GraphHopperReadPDF(environment.getProperty("BEIJING_PBF_FILE_PATH"), "./target/beijingmapmatchingtest");
    }

    /**
     * load point data and edge data from file to lists.
     * form a map from lists while some processing will be performed
     *
     * @param allEdgeMap key--edgeId(Integer) value--TorSegment instance
     * @param allPointMap key--pointId(Integer) value--TorVertex instance
     * */
    public static void getGraphMap(Map<Integer, MMEdge> allEdgeMap, Map<Integer, MMPoint> allPointMap) {
        List<MMPoint> towerPoints = new ArrayList<>();
        List<MMEdge> allEdges = new ArrayList<>();
        Dataset.readGraph(BEIJING_POINT_FILE, BEIJING_EDGE_FILE, towerPoints, allEdges);
        for (MMEdge edge : allEdges) {
            edge.convertFromDatabaseForm();
            allEdgeMap.put(edge.getId(), edge);
        }
        for (MMPoint towerPoint : towerPoints) {
            allPointMap.put(towerPoint.getId(), towerPoint);
        }
        allEdges.clear();
        towerPoints.clear();
    }

    /**
     *
     *
     * input file:  BEIJING DATA/EFFICIENCY_QUERY_FILE
     * output file: BEIJING DATA/EFFICIENCY_QUERY_FILE.20
     *              BEIJING DATA/EFFICIENCY_QUERY_FILE.40
     *              ...
     *              BEIJING DATA/EFFICIENCY_QUERY_FILE.100
     * output format: query_id( tra_id)    pointStr(points on trajectory)    pointStr(points on virtual graph)    edge1_Id,edge2_Id...
     *
     * @param toggleOff indicate whether to execute this subroutine.
     * @param force indicate whether generate in the case of the file about to generate exists
     *              if true, we are going to regenerate it no matter the file exists or not.
     *              if false and the file exists, we won't do it a second time.
     * */
    private void convertQueriesToTenForms(boolean toggleOff, boolean force) {
        if (toggleOff) return;
        if (Files.exists(Paths.get(EFFICIENCY_QUERY_FILE + "." + 20)) && !force) return;
        logger.info("Enter convertQueriesToTenForms");
        initBeijingTrajectoryMapping();
        //load query( selected trajectories generated from previous step) file
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile(EFFICIENCY_QUERY_FILE, 10000000);

        for (int i = 10; i <= 100; i += 10)
            deleteFile(EFFICIENCY_QUERY_FILE + "." + i);

        for (Trajectory query : trajectoryMap.values()) {
            List<MMPoint> points = new ArrayList<>();
            for (int i = 0; i < 100; ++i) {
                points.add(query.getMMPoints().get(i));
                if (points.size() % 20 == 0) {
                    List<MMPoint> pathPoints = new ArrayList<>();
                    List<MMEdge> edges = new ArrayList<>();
                    mapper.match(points, pathPoints, edges);
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(EFFICIENCY_QUERY_FILE + "." + (i + 1), true))) {
                        writer.write(query.getId() + "\t");
                        writer.write(Trajectory.convertToPointStr(points) + "\t");
                        writer.write(Trajectory.convertToPointStr(pathPoints) + "\t");
                        boolean first = true;
                        for (MMEdge edge : edges) {
                            if (first)
                                first = false;
                            else writer.write(",");
                            writer.write(edge.getId() + "");
                        }
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        logger.info("Exit convertQueriesToTenForms");
    }

    /**
     * construct a map that contains queries.
     * each entry of the map containing
     * - key: Integer, means number of nodes in the query.（ 0--20， 1--40 ... 4--100）
     * - values: List<Trajectory> all trajectories from one file corresponding to key.
     *
     * @param calibrated it determins which kind of point to use to represent a trajectory.
     *                   if true, points on virtual graph will be used.
     *                   if false, points on trajectory will be used.
     * @return a map each entry represents the file containing specific number of nodes in trajectories within the file.
     */
    public static Map<Integer, List<Trajectory>> loadQueryMap(boolean calibrated) {
        logger.info("Enter loadQueryMap");
        Map<Integer, List<Trajectory>> trajectoryMap = new HashMap<>();
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        for (int i = 20; i <= 100; i += 20) {
            String fileName = EFFICIENCY_QUERY_FILE + "." + i;
            int key = i / 20 - 1;
            List<Trajectory> trajectoryList = new ArrayList<>(300);
            trajectoryMap.put(key, trajectoryList);
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line = null;
                while ((line = reader.readLine()) != null) {
                    String filteredLineStr = line.replace("\"", "");

                    //format: query_id( tra_id)    pointStr(points on trajectory)    pointStr(points on virtual graph)    edge1_Id,edge2_Id...
                    String[] array = filteredLineStr.split("\t");
                    int trajectoryID = Integer.parseInt(array[0]);
                    String pointStr = calibrated ? array[2] : array[1];
                    Trajectory trajectory = new Trajectory(trajectoryID, pointStr, array[3]);
                    trajectory.getMMPoints();
                    trajectoryList.add(trajectory);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit loadQueryMap");
        return trajectoryMap;
    }


    public static Map<Integer, Short> loadCalibratedTrajectoryLengthMap() {

        Map<Integer, Short> res = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("CALIBRATED_TRAJECTORY_LENGTH"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] array = line.split(Common.instance.SEPARATOR2);
                res.put(Integer.parseInt(array[0]), Short.parseShort(array[1]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * write bytes to file.
     * @param n
     * @param buffer
     * @param writer
     * @throws IOException
     */
    private static void writeInt(int n, ByteBuffer buffer, OutputStream writer) throws IOException {
        buffer.putInt(n);
        writer.write(buffer.array());
        buffer.clear();
    }

    /**
     * delete the file if exists
     * @param name URI of the file
     */
    private static void deleteFile(String name) {
        try {
            Files.deleteIfExists(Paths.get(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * customized Map.Entry<K,V>
     */
    class Pair {
        final int key;
        final double score;

        Pair(int key, double score) {
            this.key = key;
            this.score = score;
        }
    }
}
