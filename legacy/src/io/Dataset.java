package au.edu.rmit.trajectory.torch.io;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.service.MMEdgeService;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import au.edu.rmit.trajectory.torch.queryEngine.similarity.DistanceFunction;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.storage.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author forrest0402
 * @Description
 * @date 12/29/2017
 */
@Component
public class Dataset {

    private static Logger logger = LoggerFactory.getLogger(Dataset.class);

    @Autowired
    TrajectoryService trajectoryService;

    @Autowired
    MMEdgeService edgeService;

    @Autowired
    Mapper mapper;

    @Autowired
    Environment environment;

    @Autowired
    FileManager fileManager;

    final static String PREFIX = "./exp/beijing effectiveness/";

    final String TRAJ_ID_PATH = PREFIX + "trajectory.hash.200000.txt";

    final static String QUERY_PATH = PREFIX + "QUERY.ID";

    final int K = 100;

    final String LCSS_FILE_NAME1 = PREFIX + "lcss.top" + K;
    final String EDR_FILE_NAME1 = PREFIX + "edr.top" + K;
    final String DTW_FILE_NAME1 = PREFIX + "dtw.top" + K;

    final String LCSS_FILE_NAME2 = PREFIX + "mm.lcss.top" + K;
    final String EDR_FILE_NAME2 = PREFIX + "mm.edr.top" + K;
    final String DTW_FILE_NAME2 = PREFIX + "mm.dtw.top" + K;

    final String LCSS_FILE_NAME3 = PREFIX + "mm.aligned.lcss.top" + K;
    final String EDR_FILE_NAME3 = PREFIX + "mm.aligned.edr.top" + K;
    final String DTW_FILE_NAME3 = PREFIX + "mm.aligned.dtw.top" + K;

    final double DISTANCE_THREASHOLD = 2000;

    public void readBeijingDataset(String filePath, int limit) {
        AtomicInteger idGenerator = new AtomicInteger(1);
        try (BufferedReader bw = new BufferedReader(new FileReader(filePath))) {
            String lineStr = null;
            StringBuilder trajectoryBuilder = new StringBuilder();
            trajectoryBuilder.append("[");
            List<Trajectory> trajectoryList = new ArrayList<>(limit);
            int pointNumber = 0;
            while ((lineStr = bw.readLine()) != null) {
                if (limit <= 0) break;
                if (idGenerator.intValue() % 100 == 0)
                    System.out.println("process: " + idGenerator.intValue());
                String[] lineArray = lineStr.split("\t");
                double lat = Double.parseDouble(lineArray[3]);
                double lon = Double.parseDouble(lineArray[2]);
                if (lon == 0.0 || lat == 0.0) {
                    if (pointNumber > 4) {
                        trajectoryBuilder.append("]");
                        Trajectory trajectory = new Trajectory(trajectoryBuilder.toString());
                        trajectory.setId(idGenerator.getAndIncrement());
                        trajectoryList.add(trajectory);
                        --limit;
                    }
                    trajectoryBuilder = new StringBuilder();
                    trajectoryBuilder.append("[");
                    pointNumber = 0;
                } else {
                    if (trajectoryBuilder.length() > 1)
                        trajectoryBuilder.append(",");
                    trajectoryBuilder.append("[").append(lon).append(",").append(lat).append("]");
                    ++pointNumber;
                }
            }
            logger.info("start to insert");
            trajectoryService.insertTrajectories(trajectoryList);
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    public void generateSegmentsOfBeijing() {
        Graph graph = mapper.getGraph(environment.getProperty("BEIJING_PBF_FILE_PATH"), "./target/beijingmapmatchingtest");
        GraphHopper hopper = mapper.getHopper();
        System.out.println(graph.getNodes());
        //preprocess all edges
        Map<Integer, MMEdge> allEdges = new HashMap<>();
        Map<Integer, AllEdgesIterator> allOriginalEdges = new HashMap<>();
        System.out.println(graph.getAllEdges().getMaxId());
        AllEdgesIterator allEdgeIterator = graph.getAllEdges();
        boolean[] visit = new boolean[79365];
        while (allEdgeIterator.next()) {
            MMEdge edge = new MMEdge(allEdgeIterator, hopper);
            visit[allEdgeIterator.getEdge()] = true;
            edge.convertToDatabaseForm();
            if (!allEdges.containsKey(edge.getId())) {
                allEdges.put(edge.getId(), edge);
                allOriginalEdges.put(edge.getId(), allEdgeIterator);
            } else {

            }
        }
        List<MMEdge> edges = new ArrayList<>(allEdges.values());
        logger.info("edge size: {}", edges.size());
        edgeService.deleteAllMMEdges();
        edgeService.insertMMEdges(edges);
    }

    private void generateQueryIDs() {
        //trajectoryMapping.GraphHopperReadPDF(environment.getProperty("BEIJING_PBF_FILE_PATH"), "./target/beijingmapmatchingtest");
        SecureRandom random = new SecureRandom();
        Set<Integer> ids = new HashSet<>();
        List<Trajectory> trajectoryList = trajectoryService.getAllTrajectories();
        Map<Integer, MMEdge> allEdges = edgeService.getAllEdges();
        while (ids.size() < 1500) {
            Trajectory trajectory = trajectoryList.get(random.nextInt(trajectoryList.size()));
            List<MMEdge> edges = trajectory.getMapMatchedTrajectory(allEdges);
            if (edges.size() > 0)
                ids.add(trajectory.getId());
        }
        try {
            fileManager.writeToFile(QUERY_PATH, new ArrayList<>(ids), false);
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    private void generateTrajectoryIDs() {
        List<Trajectory> trajectoryList = trajectoryService.getAllTrajectories();
        Map<Integer, MMEdge> allEdges = edgeService.getAllEdges();
        Set<Integer> ids = new HashSet<>();
        for (Trajectory trajectory : trajectoryList) {
            List<MMEdge> edges = trajectory.getMapMatchedTrajectory(allEdges);
            if (edges.size() > 0)
                ids.add(trajectory.getId());
        }
        try {
            fileManager.writeToFile(TRAJ_ID_PATH, new ArrayList<>(ids), false);
        } catch (IOException e) {
            logger.error("{}", e);
        }
    }

    public static List<Integer> loadQuery() {
        List<Integer> trajectoryIds = new ArrayList<>();
        try {
            //Files.lines(Paths.get(QUERY_PATH)).forEach(line -> trajectoryIds.add(Integer.parseInt(line)));
            BufferedReader reader = new BufferedReader(new FileReader(QUERY_PATH));
            String line = null;
            while ((line = reader.readLine()) != null) {
                trajectoryIds.add(Integer.parseInt(line));
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
        return trajectoryIds;
    }

    public void mapMatchingRawTrajectories() {
        logger.info("load query");
        List<Trajectory> trajectoryList = trajectoryService.getAllTrajectories();
        Map<Integer, MMEdge> allEdges = new HashMap<>();
        List<MMEdge> edges = edgeService.getAllMMEdges();
        for (MMEdge edge : edges) {
            allEdges.put(edge.getId(), edge);
        }
        edges.clear();
        logger.info("start to map matching");
        mapper.GraphHopperReadPDF(environment.getProperty("BEIJING_PBF_FILE_PATH"), "./target/beijingmapmatchingtest");
        int failNumber = 0;
        int updateBulkNumber = 1000;
        List<Trajectory> waitList = new ArrayList<>(updateBulkNumber);

        int currentValue = 28751;
        AtomicInteger process = new AtomicInteger(currentValue);
        ExecutorService threadPool = new ThreadPoolExecutor(8, 8, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        Iterator<Trajectory> iter = trajectoryList.iterator();
        while (iter.hasNext()) {
            Trajectory trajectory = iter.next();
            if (trajectory.getId() < currentValue) {
                iter.remove();
            } else threadPool.execute(new Task(trajectory, allEdges, waitList, updateBulkNumber, process));
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }

        if (waitList.size() > 0)
            trajectoryService.updateMMEdges(waitList);
    }

    private void readTDrive(String filePath, int limit) {
        trajectoryService.delAllTrajectories();

//        logger.info("start to insert {} trajectories", trajectoryList.size());
//        trajectoryService.insertTrajectories(trajectoryList);
    }

    private void filterShortTrajectories() {
        List<Trajectory> trajectoryList = trajectoryService.getAllTrajectories();
        List<Integer> ids = new ArrayList<>();
        logger.info("start to filter");
        int process = 0;
        for (Trajectory trajectory : trajectoryList) {
            if (process++ % 100 == 0)
                System.out.println(process);
            if (trajectory.isShort(50.0) || trajectory.getPoints().size() < 3)
                ids.add(trajectory.getId());
        }
        System.out.println(ids.size());
        if (ids.size() > 0)
            trajectoryService.delTrajectories(ids);
    }

    private void deleteDuplicatePoints() {
        List<Trajectory> trajectoryList = trajectoryService.getAllTrajectories();
        for (Trajectory trajectory : trajectoryList) {
            List<MMPoint> points = trajectory.getMMPoints();
            MMPoint pre = null;
            Iterator<MMPoint> iter = points.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");
            while (iter.hasNext()) {
                MMPoint point = iter.next();
                if (point.equals(pre)) {
                    iter.remove();
                } else {
                    pre = point;
                    if (stringBuilder.length() > 1) stringBuilder.append(",");
                    stringBuilder.append("[").append(point.getLon()).append(",").append(point.getLat()).append("]");
                }
            }
            stringBuilder.append("]");
            trajectory.setPointStr(stringBuilder.toString());
        }
        trajectoryService.delAllTrajectories();
        trajectoryService.insertTrajectories(trajectoryList);
    }

    private void findTopK() {
        List<Trajectory> queryTrajList = trajectoryService.getTrajectories(loadQuery());
        logger.info("query size: {}", queryTrajList.size());
        Set<Integer> trajectoryIds = new HashSet<>();
        try {
            Files.lines(Paths.get(TRAJ_ID_PATH)).forEach(line -> {
                try {
                    if (trajectoryIds.size() < 150000)
                        trajectoryIds.add(Integer.parseInt(line));
                } catch (NumberFormatException e) {
                    logger.error("{}, {}", line, e);
                }
            });
        } catch (IOException e) {
            logger.error("{}", e);
        }
        List<Trajectory> trajectoryList = trajectoryService.getTrajectories(new ArrayList<>(trajectoryIds));
        Map<Integer, MMEdge> allEdges = edgeService.getAllEdges();
        for (Trajectory trajectory : trajectoryList) {
            trajectory.getMapMatchedTrajectory(allEdges);
        }
        for (Trajectory trajectory : queryTrajList) {
            trajectory.getMapMatchedTrajectory(allEdges);
        }
        allEdges.clear();
        logger.info("prepare to find top k");
        AtomicInteger process = new AtomicInteger(0);
        ExecutorService threadPool = new ThreadPoolExecutor(6, 6, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (Trajectory trajectory : queryTrajList) {
            threadPool.execute(new TopKTask(trajectory, trajectoryList, process));
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
    }

    final static Map<Integer, Trajectory> TRAJ_DATABASE = new HashMap<>();

    final static Map<Integer, MMEdge> EDGE_DATABASE = new HashMap<>();

    final static String PORTO_POINT_FILE = "PORTO_POINT_FILE";
    final static String PORTO_EDGE_FILE = "PORTO_EDGE_FILE";

    final static String SEPERATOR = Common.instance.SEPARATOR2;

    /**
     * store points on virtual graph modeled by TorVertex and edges  on virtual graph modeled by MMEdges to disk.
     * */
    public static void storeGraph(String pointFile, String edgeFile, Collection<MMPoint> towerPoints, Collection<MMEdge> allEdges) {
        if (Files.exists(Paths.get(pointFile)) && Files.exists(Paths.get(edgeFile))) return;
        try (BufferedWriter pointWriter = new BufferedWriter(new FileWriter(pointFile));
             BufferedWriter edgeWriter = new BufferedWriter(new FileWriter(edgeFile))) {
            for (MMPoint towerPoint : towerPoints) {
                pointWriter.write(towerPoint.getId() + SEPERATOR + towerPoint.getLat() + SEPERATOR + towerPoint.getLon());
                pointWriter.newLine();
            }
            for (MMEdge edge : allEdges) {
                edgeWriter.write(edge.convertToDatabaseForm());
                edgeWriter.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * preprocess point data and edge data from file.
     *
     * @param pointFile path where the point data file is stored.
     * @param edgeFile  path to the edge data file
     * @param towerPoints a collection of points preprocess from point data file.
     *                    this will be filled in the subroutine
     * @param allEdges a collection of edges preprocess from edge data file.
     *                 this will be filled in the subroutine
     * */
    public static void readGraph(String pointFile, String edgeFile, Collection<MMPoint> towerPoints, Collection<MMEdge> allEdges) {
        try (BufferedReader pointReader = new BufferedReader(new FileReader(pointFile));
             BufferedReader edgeReader = new BufferedReader(new FileReader(edgeFile))) {
            String line;
            while ((line = pointReader.readLine()) != null) {
                String[] array = line.split(SEPERATOR);
                int id = Integer.parseInt(array[0]);
                double latitude = Double.parseDouble(array[1]);
                double longtitude = Double.parseDouble(array[2]);
                towerPoints.add(new MMPoint(latitude, longtitude));
            }
            while ((line = edgeReader.readLine()) != null) {
                String[] array = line.split(SEPERATOR);
                int id = Integer.parseInt(array[0]);
                String lats = array[1];
                String lngs = array[2];
                double length = Double.parseDouble(array[3]);
                boolean isForward = Boolean.parseBoolean(array[4]);
                boolean isBackward = Boolean.parseBoolean(array[5]);
                MMEdge edge = new MMEdge(id, lats, lngs, length, isForward, isBackward);
                edge.convertFromDatabaseForm();
                allEdges.add(edge);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void getPortoGraph(List<MMPoint> towerPoints, List<MMEdge> allEdges) {
        if (towerPoints == null || allEdges == null)
            throw new IllegalArgumentException("towerVertexes and allEdges cannot be null");
        readGraph(PORTO_POINT_FILE, PORTO_EDGE_FILE, towerPoints, allEdges);
    }

    public static void storePortoGraph(Collection<MMPoint> towerPoints, Collection<MMEdge> allEdges) {
        logger.info("Enter storePortoGraph");
        storeGraph(PORTO_POINT_FILE, PORTO_EDGE_FILE, towerPoints, allEdges);
        logger.info("Exit storePortoGraph");
    }

    public static void updateMysqlTrajTxtFile(String trajectoryFile, Collection<Trajectory> trajectoryList) {
        try {
            Files.deleteIfExists(Paths.get(trajectoryFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(trajectoryFile))) {
            for (Trajectory trajectory : trajectoryList) {
                writer.write(trajectory.convertToDatabaseForm());
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * load trajectory data from file.
     *
     * @param trajectoryFile path to the trajectoryFile
     * @param limit number of trajectories to load
     * */
    public static Map<Integer, Trajectory> loadMysqlTrajTxtFile(String trajectoryFile, int limit) {
        logger.info("start to preprocess trajecties from {}", trajectoryFile);
        Map<Integer, Trajectory> res = new HashMap<>();
        int totalLimit = limit;
        try {
            ExecutorService threadPool = new ThreadPoolExecutor(10, 10, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            //Files.lines(Paths.get(Dataset.class.getResource("/trajectory.txt").toURI())).forEach(line -> {
            //BufferedReader reader = new BufferedReader(new InputStreamReader(Dataset.class.getClassLoader().getResourceAsStream(trajectoryFile)));
            BufferedReader reader = new BufferedReader(new FileReader(trajectoryFile));
            String line = null;
            AtomicInteger process = new AtomicInteger(0);
            while ((line = reader.readLine()) != null) {
                final String filteredLineStr = line.replace("\"", "");
                if (limit-- <= 0) break;
                threadPool.execute(() -> {
                    if (process.incrementAndGet() % 50000 == 0)
                        logger.info("Reading {} - {}", process.intValue(), trajectoryFile);
                    String[] array = filteredLineStr.split("\t");
                    int id = Integer.parseInt(array[0]);
                    String edgeStr = array.length == 3 ? array[2] : null;
                    Trajectory trajectory = new Trajectory(id, array[1], edgeStr);
                    if (edgeStr != null) {
                        //trajectory.getMapMatchedTrajectory(res);
                        //trajectory.getMMPoints();
                        //trajectory.getMatchedPoints();
                        //trajectory.getMatchedAlignedPoints();
                        trajectory.getPoints();
                    } else {
                        trajectory.getPoints();
                    }
                    synchronized (Dataset.class) {
                        if (edgeStr != null)
                            res.put(id, trajectory);
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
        logger.info("Exit loadMysqlTrajTxtFile - {}", res.size());
        return res;
    }

    public static Map<Integer, Trajectory> loadMysqlTrajTxtFile(String trajectoryFile, Map<Integer, MMEdge> allEdges, int limit) {
        logger.info("start to preprocess trajecties from txt");
        Map<Integer, Trajectory> res = new HashMap<>();
        int totalLimit = limit;
        try {
            ExecutorService threadPool = new ThreadPoolExecutor(10, 10, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            BufferedReader reader = new BufferedReader(new FileReader(trajectoryFile));
            String line = null;
            AtomicInteger process = new AtomicInteger(0);
            while ((line = reader.readLine()) != null) {
                final String filteredLineStr = line.replace("\"", "");
                if (limit-- <= 0) break;
                threadPool.execute(() -> {
                    if (process.incrementAndGet() % 20000 == 0)
                        logger.info("Reading {} - {}", process.intValue(), trajectoryFile);
                    String[] array = filteredLineStr.split("\t");
                    int id = Integer.parseInt(array[0]);
                    String edgeStr = array.length == 3 ? array[2] : null;
                    Trajectory trajectory = new Trajectory(id, array[1], edgeStr);
                    if (edgeStr != null) {
                        trajectory.getMapMatchedTrajectory(allEdges);
                        trajectory.getMMPoints();
                        trajectory.getMatchedPoints();
                        trajectory.getMatchedAlignedPoints();
                        trajectory.getPoints();
                    } else {
                        trajectory.getPoints();
                    }
                    synchronized (Dataset.class) {
                        res.put(id, trajectory);
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
        logger.info("Exit loadMysqlTrajTxtFile");
        return res;
    }

    public static void loadMysqlTxtFile() {
        URL url = Dataset.class.getResource("/edge.txt");
        logger.info("start to preprocess edges from txt: " + url);
        try {
            //Files.lines(Paths.get(this.getClass().getClassLoader().getResource("edge.txt").toURI())).forEach(line -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(Dataset.class.getClass().getResourceAsStream("/edge.txt")));
            String line = null;
            while ((line = reader.readLine()) != null) {
                line = line.replace("\"", "");
                String[] array = line.split("\t");
                int id = Integer.parseInt(array[0]);
                double length = Double.parseDouble(array[3]);
                boolean isForward = "1".equals(array[4]);
                boolean isBackward = "1".equals(array[5]);
                MMEdge edge = new MMEdge(id, array[1], array[2], length, isForward, isBackward);
                edge.convertFromDatabaseForm();
                EDGE_DATABASE.put(id, edge);
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
        logger.info("start to preprocess trajecties from txt");
        try {
            ExecutorService threadPool = new ThreadPoolExecutor(10, 10, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            //Files.lines(Paths.get(Dataset.class.getResource("/trajectory.txt").toURI())).forEach(line -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(Dataset.class.getClass().getResourceAsStream("/trajectory.txt")));
            String line = null;
            while ((line = reader.readLine()) != null) {
                final String filteredLineStr = line.replace("\"", "");
                threadPool.execute(() -> {
                    String[] array = filteredLineStr.split("\t");
                    int id = Integer.parseInt(array[0]);
                    String edgeStr = array.length == 3 ? array[2] : null;
                    Trajectory trajectory = new Trajectory(id, array[1], edgeStr);
                    if (edgeStr != null) {
                        trajectory.getMapMatchedTrajectory(EDGE_DATABASE);
                        trajectory.getMMPoints();
                        trajectory.getMatchedPoints();
                        trajectory.getMatchedAlignedPoints();
                    }
                    synchronized (Dataset.class) {
                        TRAJ_DATABASE.put(id, trajectory);
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
        logger.info("Exit loading txt");
    }

    private void findTopKServerVersion() {
        loadMysqlTxtFile();
        logger.info("Enter findTopKServerVersion");
        List<Trajectory> queryTrajList = new ArrayList<>();
        List<Integer> queryIDs = loadQuery();
        for (Integer queryID : queryIDs) {
            queryTrajList.add(TRAJ_DATABASE.get(queryID));
        }

        logger.info("query size: {}", queryTrajList.size());
        Set<Integer> trajectoryIds = new HashSet<>();
        try {
            //Files.lines(Paths.get(TRAJ_ID_PATH)).forEach(line -> trajectoryIds.add(Integer.parseInt(line)));
            BufferedReader reader = new BufferedReader(new FileReader(TRAJ_ID_PATH));
            String line = null;
            while ((line = reader.readLine()) != null) {
                trajectoryIds.add(Integer.parseInt(line));
            }
        } catch (IOException e) {
            logger.error("{}", e);
        }
        List<Trajectory> trajectoryList = new ArrayList<>();
        for (Integer trajectoryId : trajectoryIds) {
            trajectoryList.add(TRAJ_DATABASE.get(trajectoryId));
        }
//        while (trajectoryList.size() > 10) {
//            trajectoryList.remove(0);
//        }
        logger.info("prepare to find top k");
        AtomicInteger process = new AtomicInteger(0);
        ExecutorService threadPool = new ThreadPoolExecutor(20, 20, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (Trajectory trajectory : queryTrajList) {
            threadPool.execute(new TopKTask(trajectory, trajectoryList, process));
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
    }

    public void readTDrive() {
        logger.info("Enter ");
        String filePath = "taxi_log_2008_by_id";
        //readTDrive(filePath, 2000000);
        generateSegmentsOfBeijing();
        logger.info("Exit");
    }

    class TopKTask implements Runnable {

        final Trajectory QUERY;

        final List<Trajectory> TRAJECTORY_DATABASE;

        final AtomicInteger process;

        final SimilarityMeasure<MMPoint> SIM_MEASURE;

        public TopKTask(Trajectory QUERY, List<Trajectory> TRAJECTORY_DATABASE, AtomicInteger process) {
            this.QUERY = QUERY;
            this.TRAJECTORY_DATABASE = TRAJECTORY_DATABASE;
            this.process = process;
            DistanceFunction<MMPoint, MMPoint> distFunc = (p1, p2) -> GeoUtil.distance(p1, p2);
            Comparator<MMPoint> comparator = (p1, p2) -> {
                double dist = GeoUtil.distance(p1, p2);
                if (dist < 8) return 0;
                return 1;
            };
            this.SIM_MEASURE = new SimilarityMeasure<>(distFunc, comparator);
        }

        @Override
        public void run() {
            process.incrementAndGet();
            if (process.intValue() % 10 == 0)
                logger.info("process: {}", process.intValue());

            PriorityQueue<Pair> lcssPrior1 = new PriorityQueue<>((c1, c2) -> Double.compare(c1.score, c2.score));
            PriorityQueue<Pair> lcssPrior2 = new PriorityQueue<>((c1, c2) -> Double.compare(c1.score, c2.score));
            PriorityQueue<Pair> lcssPrior3 = new PriorityQueue<>((c1, c2) -> Double.compare(c1.score, c2.score));

            PriorityQueue<Pair> dtwPrior1 = new PriorityQueue<>((c1, c2) -> Double.compare(c2.score, c1.score));
            PriorityQueue<Pair> dtwPrior2 = new PriorityQueue<>((c1, c2) -> Double.compare(c2.score, c1.score));
            PriorityQueue<Pair> dtwPrior3 = new PriorityQueue<>((c1, c2) -> Double.compare(c2.score, c1.score));

            PriorityQueue<Pair> edrPrior1 = new PriorityQueue<>(Comparator.comparingDouble(Pair::getScore).reversed());
            PriorityQueue<Pair> edrPrior2 = new PriorityQueue<>(Comparator.comparingDouble(Pair::getScore).reversed());
            PriorityQueue<Pair> edrPrior3 = new PriorityQueue<>(Comparator.comparingDouble(Pair::getScore).reversed());


            for (Trajectory trajectory : TRAJECTORY_DATABASE) {
                double lcssScore1 = this.SIM_MEASURE.LongestCommonSubsequence(QUERY.getMMPoints(), trajectory.getMMPoints(), Integer.MAX_VALUE);
                double lcssScore2 = this.SIM_MEASURE.LongestCommonSubsequence(QUERY.getMatchedPoints(), trajectory.getMatchedPoints(), Integer.MAX_VALUE);
                double lcssScore3 = this.SIM_MEASURE.LongestCommonSubsequence(QUERY.getMatchedAlignedPoints(), trajectory.getMatchedAlignedPoints(), Integer.MAX_VALUE);


                lcssPrior1.add(new Pair(trajectory.getId(), lcssScore1));
                lcssPrior2.add(new Pair(trajectory.getId(), lcssScore2));
                lcssPrior3.add(new Pair(trajectory.getId(), lcssScore3));

                if (lcssPrior1.size() > K) lcssPrior1.poll();
                if (lcssPrior2.size() > K) lcssPrior2.poll();
                if (lcssPrior3.size() > K) lcssPrior3.poll();


                double dtwScore1 = this.SIM_MEASURE.DynamicTimeWarping(QUERY.getMMPoints(), trajectory.getMMPoints());
                double dtwScore2 = this.SIM_MEASURE.DynamicTimeWarping(QUERY.getMatchedPoints(), trajectory.getMatchedPoints());
                double dtwScore3 = this.SIM_MEASURE.DynamicTimeWarping(QUERY.getMatchedAlignedPoints(), trajectory.getMatchedAlignedPoints());


                dtwPrior1.add(new Pair(trajectory.getId(), dtwScore1));
                dtwPrior2.add(new Pair(trajectory.getId(), dtwScore2));
                dtwPrior3.add(new Pair(trajectory.getId(), dtwScore3));

                if (dtwPrior1.size() > K) dtwPrior1.poll();
                if (dtwPrior2.size() > K) dtwPrior2.poll();
                if (dtwPrior3.size() > K) dtwPrior3.poll();


                double edrScore1 = this.SIM_MEASURE.EditDistanceonRealSequence(QUERY.getMMPoints(), trajectory.getMMPoints());
                double edrScore2 = this.SIM_MEASURE.EditDistanceonRealSequence(QUERY.getMatchedPoints(), trajectory.getMatchedPoints());
                double edrScore3 = this.SIM_MEASURE.EditDistanceonRealSequence(QUERY.getMatchedAlignedPoints(), trajectory.getMatchedAlignedPoints());


                edrPrior1.add(new Pair(trajectory.getId(), edrScore1));
                edrPrior2.add(new Pair(trajectory.getId(), edrScore2));
                edrPrior3.add(new Pair(trajectory.getId(), edrScore3));

                if (edrPrior1.size() > K) edrPrior1.poll();
                if (edrPrior2.size() > K) edrPrior2.poll();
                if (edrPrior3.size() > K) edrPrior3.poll();
            }

            StringBuilder lcssTopK1 = new StringBuilder();
            buildString(lcssTopK1, lcssPrior1);

            StringBuilder lcssTopK2 = new StringBuilder();
            buildString(lcssTopK1, lcssPrior2);

            StringBuilder lcssTopK3 = new StringBuilder();
            buildString(lcssTopK3, lcssPrior3);

            StringBuilder dtwTopK1 = new StringBuilder();
            buildString(dtwTopK1, dtwPrior1);

            StringBuilder dtwTopK2 = new StringBuilder();
            buildString(dtwTopK2, dtwPrior2);

            StringBuilder dtwTopK3 = new StringBuilder();
            buildString(dtwTopK3, dtwPrior3);

            StringBuilder edrTopK1 = new StringBuilder();
            buildString(edrTopK1, edrPrior1);

            StringBuilder edrTopK2 = new StringBuilder();
            buildString(edrTopK2, edrPrior2);

            StringBuilder edrTopK3 = new StringBuilder();
            buildString(edrTopK3, edrPrior3);
            try {
                synchronized (Dataset.class) {
                    fileManager.writeToFile(LCSS_FILE_NAME1, lcssTopK1.toString(), true);
                    fileManager.writeToFile(LCSS_FILE_NAME2, lcssTopK2.toString(), true);
                    fileManager.writeToFile(LCSS_FILE_NAME3, lcssTopK3.toString(), true);
                    fileManager.writeToFile(DTW_FILE_NAME1, dtwTopK1.toString(), true);
                    fileManager.writeToFile(DTW_FILE_NAME2, dtwTopK2.toString(), true);
                    fileManager.writeToFile(DTW_FILE_NAME3, dtwTopK3.toString(), true);
                    fileManager.writeToFile(EDR_FILE_NAME1, edrTopK1.toString(), true);
                    fileManager.writeToFile(EDR_FILE_NAME2, edrTopK2.toString(), true);
                    fileManager.writeToFile(EDR_FILE_NAME3, edrTopK3.toString(), true);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void buildString(StringBuilder stringBuilder, PriorityQueue<Pair> prior) {
            stringBuilder.append(QUERY.getId()).append(" ");
            while (prior.size() > 0) {
                Pair elem = prior.poll();
                stringBuilder.append(elem.trajectoryId).append(",").append(elem.score).append(" ");
            }
        }

    }

    class Pair {
        public int trajectoryId;
        public double score;

        public Pair(int trajectoryId, double score) {
            this.trajectoryId = trajectoryId;
            this.score = score;
        }

        public double getScore() {
            return score;
        }
    }

    class Task implements Runnable {

        final Trajectory trajectory;

        final Map<Integer, MMEdge> allEdges;

        final List<Trajectory> waitList;

        final int updateBulkNumber;

        final AtomicInteger process;

        Task(Trajectory trajectory, Map<Integer, MMEdge> allEdges, List<Trajectory> waitList, int updateBulkNumber, AtomicInteger process) {
            this.trajectory = trajectory;
            this.allEdges = allEdges;
            this.waitList = waitList;
            this.updateBulkNumber = updateBulkNumber;
            this.process = process;
        }

        @Override
        public void run() {
            if (process.intValue() % 10 == 0)
                logger.info("current process: {}", process.intValue());
            try {
                List<MMEdge> rawEdges = mapper.match(trajectory.getPoints());
                StringBuilder edgeStr = new StringBuilder(rawEdges.size());
                synchronized (Dataset.class) {
                    for (MMEdge rawEdge : rawEdges) {
                        MMEdge edge = allEdges.get(rawEdge.hashCode());
                        if (edge == null) {
                            rawEdge.convertToDatabaseForm();
                            edgeService.insertMMEdge(rawEdge);
                            allEdges.put(rawEdge.getId(), rawEdge);
                        }
                        if (edgeStr.length() > 0)
                            edgeStr.append(Common.instance.SEPARATOR);
                        edgeStr.append(rawEdge.getId());
                    }
                }
                trajectory.setEdgeStr(edgeStr.toString());
                waitList.add(trajectory);
                process.incrementAndGet();
            } catch (java.lang.IllegalArgumentException e) {
                //logger.error("{}", e);
            }
            synchronized (Dataset.class) {
                if (waitList.size() == updateBulkNumber) {
                    trajectoryService.updateMMEdges(waitList);
                    waitList.clear();
                }
            }
        }
    }
}
