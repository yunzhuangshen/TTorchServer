package au.edu.rmit.trajectory.similarity;

import au.edu.rmit.trajectory.similarity.algorithm.*;
import au.edu.rmit.trajectory.similarity.datastructure.NodeGridIndex;
import au.edu.rmit.trajectory.similarity.datastructure.EdgeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.NodeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.TraGridIndex;
import au.edu.rmit.trajectory.torch.io.BeijingDataset;
import au.edu.rmit.trajectory.torch.io.FileManager;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.datastructure.RTreeWrapper;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author forrest0402
 * @Description
 * @date 1/22/2018
 */
@Component
public class Effectiveness {

    private static Logger logger = LoggerFactory.getLogger(Effectiveness.class);
    @Autowired
    FastTimeSeriesEvaluation FTSE;

    @Autowired
    public Mapper mapper;

    @Autowired
    public TrajectoryService trajectoryService;

    @Autowired
    public Environment environment;

    @Autowired
    public FileManager fileManager;

    @Autowired
    NodeInvertedIndex nodeInvertedIndex;

    @Autowired
    EdgeInvertedIndex edgeInvertedIndex;

    @Autowired
    TraGridIndex gridIndex;

    @Autowired
    RTreeWrapper envelopeIndex;

    @Autowired
    NodeGridIndex nodeGridIndex;

    @Autowired
    Transformation transformation;

    @Autowired
    LongestCommonSubsequence longestCommonSubsequence;

    final float minLat = 41.108936f, maxLat = 41.222659f, minLon = -8.704896f, maxLon = -8.489324f;

    private void findTopK(boolean calibrated, MeasureType measureType) {
        logger.info("Enter findTop100Hausdorff - {}, {}", calibrated, measureType.toString());
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        //read all trajectory
        Map<Integer, Trajectory> trajectoryMap = BeijingDataset.loadBeijingTrajectoryTxtFile(calibrated);
        Map<Integer, List<Trajectory>> queryMap = BeijingDataset.loadQueryMap(calibrated);
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("trajectoryDatabase size: {}, queryList size:{}", trajectoryMap.size(), queryList.size());
        //find top k
        int k = 150;
        ExecutorService threadPool = new ThreadPoolExecutor(40, 40, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0);
        String rawFileName = measureType.toString() + ".top" + k;
        if (calibrated)
            rawFileName = "mm." + rawFileName;
        final String outputStr = rawFileName;
        for (Trajectory query : queryList) {
            threadPool.execute(() -> {
                try {
                    process.incrementAndGet();
                    if (process.intValue() % 100 == 0) logger.info("process: {}", process.intValue());
                    PriorityQueue<Pair> topkHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p2.value, p1.value));
                    if (measureType == MeasureType.LORS) {
                        topkHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p1.value, p2.value));
                    }
                    for (Trajectory candidateTraj : trajectoryMap.values()) {
                        double score = Double.MAX_VALUE;
                        try {
                            switch (measureType) {
                                case Hausdorff:
                                    score = similarityMeasure.Hausdorff(query.getMMPoints(), candidateTraj.getMMPoints());
                                    break;
                                case Frechet:
                                    score = similarityMeasure.Frechet(query.getMMPoints(), candidateTraj.getMMPoints());
                                    break;
                            }
                        } catch (Error e) {
                            Transformation.printPoints(query.getMMPoints());
                            Transformation.printPoints(candidateTraj.getMMPoints());
                            e.printStackTrace();
                        }
                        topkHeap.add(new Pair(candidateTraj.getId(), score));
                        if (topkHeap.size() > k) topkHeap.poll();
                    }
                    synchronized (Effectiveness.class) {
                        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputStr, true))) {
                            bw.write(query.getId() + "");
                            while (topkHeap.size() > 0) {
                                Pair topPair = topkHeap.poll();
                                bw.write(" " + topPair.key + "," + topPair.value);
                            }
                            bw.newLine();
                        }
                    }
                } catch (IOException e) {
                    logger.error("{}", e);
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
        logger.info("Exit findTop100Hausdorff");
    }

    private void testlors(Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMEdge> allEdgeMap, List<Trajectory> queryList) {
        List<MMEdge> edge1 = queryList.get(1).getMapMatchedTrajectory(allEdgeMap);
        List<MMEdge> edge2 = trajectoryMap.get(133667).getMapMatchedTrajectory(allEdgeMap);
        double score = longestCommonSubsequence.mmRun(edge1, edge2, Integer.MAX_VALUE);
        System.out.println(score);
    }

    private void findTopK() {
        logger.info("Enter findTopK");
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        BeijingDataset.getGraphMap(allEdgeMap, allPointMap);
        //read all trajectory
        Map<Integer, Trajectory> trajectoryMap = BeijingDataset.loadBeijingTrajectoryTxtFile(true);
        Map<Integer, List<Trajectory>> queryMap = BeijingDataset.loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        int k = 150;
        final String outputStr = "lors.top" + k;
        //testlors(trajectoryMap, allEdgeMap, queryList);
        try (BufferedReader reader = new BufferedReader(new FileReader(outputStr))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int id = Integer.parseInt(line.split(" ")[0]);
                Iterator<Trajectory> iter = queryList.iterator();
                while (iter.hasNext()) {
                    Trajectory next = iter.next();
                    if (next.getId() == id)
                        iter.remove();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("queryList size: {}", queryList.size());
        ExecutorService threadPool = new ThreadPoolExecutor(40, 40, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0);
        logger.info("start to find");
        edgeInvertedIndex.load();
        for (Trajectory query : queryList) {
            final List<MMEdge> querySegments = query.getMapMatchedTrajectory(allEdgeMap);
            threadPool.execute(() -> {
                try {
                    process.incrementAndGet();
                    if (process.intValue() % 1 == 0) logger.info("process: {}", process.intValue());
                    double[] restDistance = new double[querySegments.size()];
                    for (int i = querySegments.size() - 2; i >= 0 && i + 1 < querySegments.size(); --i) {
                        restDistance[i] = restDistance[i + 1] + querySegments.get(i + 1).getLength();
                    }
                    PriorityQueue<Map.Entry<Integer, Double>> topkHeap = edgeInvertedIndex.findTopK(trajectoryMap, querySegments, 60, allEdgeMap, restDistance);
                    synchronized (Effectiveness.class) {
                        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputStr, true))) {
                            bw.write(query.getId() + "");
                            while (topkHeap.size() > 0) {
                                Map.Entry<Integer, Double> topPair = topkHeap.poll();
                                bw.write(" " + topPair.getKey() + "," + topPair.getValue());
                            }
                            bw.newLine();
                        }
                    }
                } catch (IOException e) {
                    logger.error("{}", e);
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
        logger.info("Exit findTop100Hausdorff");
    }

    private void generateFrechetData() {
        Map<Integer, Trajectory> trajectoryMap = BeijingDataset.loadBeijingTrajectoryTxtFile(false);
        List<Trajectory> queryList = BeijingDataset.loadQueryMap(false).get(2);
        Map<Integer, Trajectory> queryMap = new HashMap<>();
        for (Trajectory trajectory : queryList) {
            queryMap.put(trajectory.getId(), trajectory);
        }
        final String PREFIX = "exp/beijing effectiveness/";
        String fileName = PREFIX + "top150.Hausdorff";
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
             BufferedWriter writer = new BufferedWriter(new FileWriter(PREFIX + "top150.Frechet", false))) {
            String line;
            ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            AtomicInteger process = new AtomicInteger(0);
            ReentrantLock reentrantLock = new ReentrantLock();
            while ((line = reader.readLine()) != null) {
                String[] array = line.split(" ");
                threadPool.execute(() -> {
                    try {
                        if (process.getAndIncrement() % 10 == 0)
                            System.out.println(process.intValue());
                        int queryID = Integer.parseInt(array[0]);
                        Trajectory query = queryMap.get(queryID);
                        try {
                            reentrantLock.lock();
                            writer.write(queryID + "");
                        } finally {
                            reentrantLock.unlock();
                        }
                        StringBuilder stringBuilder = new StringBuilder();
                        for (int i = 1; i < array.length; i++) {
                            String[] pairStr = array[i].split(",");
                            int trajectoryID = Integer.parseInt(pairStr[0]);
                            Trajectory candidate = trajectoryMap.get(trajectoryID);
                            double score = similarityMeasure.Frechet(query.getMMPoints(), candidate.getMMPoints());
                            stringBuilder.append(" ").append(trajectoryID).append(",").append(score);
                        }
                        try {
                            reentrantLock.lock();
                            writer.write(stringBuilder.toString());
                            writer.newLine();
                            writer.flush();
                        } finally {
                            reentrantLock.unlock();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.error("{}", e);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void run() {
        //findTopK();
//        findTopK(true, MeasureType.Hausdorff);
//        findTopK(false, MeasureType.Hausdorff);
//        findTopK(true, MeasureType.Frechet);
        //findTopK(false, MeasureType.Frechet);

    }

    class Pair {

        public final int key;
        public final double value;

        Pair(int key, double value) {
            this.key = key;
            this.value = value;
        }

        public Integer getKey() {
            return key;
        }

        public Double getValue() {
            return value;
        }
    }
}
