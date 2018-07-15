package au.edu.rmit.trajectory.similarity;

import au.edu.rmit.trajectory.similarity.algorithm.FastTimeSeriesEvaluation;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.algorithm.Transformation;
import au.edu.rmit.trajectory.similarity.config.AppConfig;
import au.edu.rmit.trajectory.similarity.datastructure.NodeGridIndex;
import au.edu.rmit.trajectory.similarity.datastructure.EdgeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.NodeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.TraGridIndex;
import au.edu.rmit.trajectory.torch.io.Dataset;
import au.edu.rmit.trajectory.torch.io.FileManager;
import au.edu.rmit.trajectory.similarity.model.*;
import au.edu.rmit.trajectory.similarity.datastructure.RTreeWrapper;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
//import com.javamex.classmexer.MemoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

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
 * Hello world!
 */
@Component
public class AppEngine {

    private static Logger logger = LoggerFactory.getLogger(AppEngine.class);

    public static final ApplicationContext APPLICATION_CONTEXT = new AnnotationConfigApplicationContext(AppConfig.class);


    public static void main(String[] args) {
        System.out.println("Hello, world.");
    }

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

    final float minLat = 41.108936f, maxLat = 41.222659f, minLon = -8.704896f, maxLon = -8.489324f;

    final int QUERY_THRESHOLD = 300;

    final int QUERY_NUMBER_LIMIT = 50;
    /**
     * only hash
     */
    final static String EFFICIENCY_QUERY_FILE = "EFFICIENCY_QUERY_FILE";

    /**
     * complete trajectories
     */
    final static String EFFICIENCY_QUERY_FILE2 = "EFFICIENCY_QUERY_FILE_TRAJECTORY";

    public void buildGridIndexForFTSE() {
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory.txt", 10000000);
        Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
        Iterator<Trajectory> iterator = trajectoryList.iterator();
        while (iterator.hasNext()) {
            Trajectory next = iterator.next();
            if (next.getPoints().size() < 5 || next.getPoints().size() > 1500) iterator.remove();
        }
        System.out.println(trajectoryList.size());
        FTSE.init(trajectoryList, 10);
    }

    /**
     * add segmentStr attribute for each trajectory
     */
    public void mapMatchingAllTrajectories() {
        //read trajectories
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory.txt", 10000000);
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        //read road network
        mapper.readPBF(environment.getProperty("PORTO_PBF_FILE_PATH"), "./target/mapmatchingtest");
        Map<Integer, MMPoint> towerPoints = new HashMap<>(100000);
        Map<Integer, MMEdge> allEdges = new HashMap<>(100000);
        List<MMPoint> towerPointList = new ArrayList<>();
        List<MMEdge> edgeList = new ArrayList<>();
        //trajectoryMapping.getGraph(towerPointList, edgeList);

        for (MMPoint point : towerPointList) {
            towerPoints.put(point.getId(), point);
        }
        for (MMEdge edge : edgeList) {
            if (allEdges.containsKey(edge.getId())) {
                MMEdge preEdge = allEdges.get(edge.getId());
                System.out.print("");
            } else allEdges.put(edge.getId(), edge);
        }

        //save road network to txt files
        Dataset.storePortoGraph(towerPoints.values(), allEdges.values());
        logger.info("start to map matching all trajectories");
        Iterator<Trajectory> iter = trajectoryList.iterator();
        int process = 0;
        while (iter.hasNext()) {
            if (++process % 10000 == 0)
                logger.info("process: {}", process);
            Trajectory trajectory = iter.next();
            try {
                List<MMEdge> edges = new ArrayList<>();
                List<MMPoint> points = new ArrayList<>();
                mapper.fastMatchMMPoint(trajectory.getMMPoints(), points, edges);
                StringBuilder edgeStr = new StringBuilder(edges.size());
                for (MMEdge edge : edges) {
                    MMEdge mapEdge = allEdges.get(edge.getId());
                    if (edgeStr.length() > 0)
                        edgeStr.append(Common.instance.SEPARATOR);
                    edgeStr.append(edge.getId());
                }
                trajectory.setEdgeStr(edgeStr.toString());
            } catch (Exception e) {
                //logger.error("{}", e);
                iter.remove();
            }
        }
        logger.info("trajectory size after map matching: {}", trajectoryList.size());
        Dataset.updateMysqlTrajTxtFile("porto_trajectory2.txt", trajectoryList);
    }

    /**
     * increasing k for LCSS and EDR using FTSE
     */
    private void efficiencyOfFTSEVaryingK(MeasureType measureType, List<Trajectory> queryList, Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter efficiencyOfFTSEVaryingK - {}", measureType.toString());
        for (int k = 5; k <= 50; k += 15) {
            logger.info("k = {}", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                FTSE.findTopK(trajectoryMap, trajectory, k, measureType, candidateNumberList);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_K_FTSE/queryTime" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_FTSE/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfFTSEVaryingK - {}", measureType.toString());
    }

    /**
     * increasing k for LCSS and EDR using LEVI
     */
    private void efficiencyOfLEVIVaryingK(MeasureType measureType, List<Trajectory> queryList, Map<Integer, Short> trajLenMap, boolean compressed) {
        logger.info("Enter efficiencyOfLEVIVaryingK - {}, query size: {}", measureType.toString(), queryList.size());
        for (int k = 5; k <= 50; k += 15) {
            logger.info("k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                //cache the result
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                long startTime = System.nanoTime();
                //nodeIndex.findTopK(complexRoadGridIndex, trajectory, k, measureType, trajLenMap, null, null, null);
                //nodeIndex.findTopK(complexRoadGridIndex, trajectory, k, measureType, trajLenMap, null, null, null);
                nodeInvertedIndex.findTopK(nodeGridIndex, trajectory, k, measureType, trajLenMap, candidateNumberList, scannedCandidateNumberList, lookupTime, compressedTime);
                long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                if (runningTime >= 100000) {
                    logger.error("trajectory hash({}) is too long", trajectory.getId());
                    Transformation.printPoints(trajectory.getMMPoints());
                }
                queryTime.add(runningTime);
            }
            try {
                String suffix = "";
                if (compressed) {
                    suffix = "_COMPRESSED";
                    fileManager.writeToFile("EFFICIENCY_VARYING_K_LEVI" + suffix + "/compressedTime_" + measureType.toString() + k, compressedTime, false);
                }
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LEVI" + suffix + "/queryTime_" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LEVI" + suffix + "/lookupTime_" + measureType.toString() + k, lookupTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LEVI" + suffix + "/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LEVI" + suffix + "/scannedCandidateNumber_" + measureType.toString() + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfLEVIVaryingK - {}", measureType.toString());
    }

    /**
     * increasing k for LCSS and EDR using LEVI
     */
    private void efficiencyOfLORSVaryingK(List<Trajectory> queryList, Map<Integer, MMEdge> allEdges, boolean compressed) {
        logger.info("Enter efficiencyOfLORSVaryingK");
        for (int k = 5; k <= 50; k += 15) {
            logger.info("***************k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            List<Integer> fullyScanNumList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() == QUERY_THRESHOLD) break;
                //cache the results
                List<MMEdge> originalSegments = trajectory.getMapMatchedTrajectory(allEdges);
                List<Edge> querySegments = new ArrayList<>();
                for (MMEdge originalSegment : originalSegments) {
                    querySegments.add(originalSegment);
                }
                double[] restDistance = new double[querySegments.size()];
                for (int i = querySegments.size() - 2; i >= 0 && i + 1 < querySegments.size(); --i) {
                    restDistance[i] = restDistance[i + 1] + querySegments.get(i + 1).getLength();
                }
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                edgeInvertedIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, false, fullyScanNumList, compressedTime);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false,compressedTime);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false,compressedTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                if (runningTime >= 10000) {
                    Transformation.printPoints(trajectory.getMMPoints());
                    //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, true);
                    logger.error("trajectory hash({}) is too long", trajectory.getId());
                }
                queryTime.add(runningTime);
            }
            try {
                String suffix = "";
                if (compressed) {
                    suffix = "_COMPRESSED";
                    fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/compressedTime_" + k, compressedTime, false);
                }
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/fullyScanNumList_" + k, fullyScanNumList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/queryTime_" + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/lookupTime_" + k, lookupTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/candidateNumberList_" + k, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_LORS" + suffix + "/scannedCandidateNumber_" + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfLORSVaryingK");
    }

    /**
     * increasing k for DTW using grid dataStructure
     */
    private void efficiencyOfDTWVaryingK(Map<Integer, Trajectory> trajectoryMap, MeasureType measureType, List<Trajectory> queryList) {
        logger.info("Enter efficiencyOfRealDistanceVaryingK");
        for (int k = 5; k <= 50; k += 15) {
            logger.info("***************k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Long> lookupTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                //cache the results
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", candidate number: " + candidateNumberList.get(candidateNumberList.size() - 1) + ", scannedCandidate number: " + scannedCandidateNumberList.get(scannedCandidateNumberList.size() - 1) + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                gridIndex.findTopK(trajectoryMap, trajectory, k, measureType, candidateNumberList, scannedCandidateNumberList, lookupTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_K_GRID_INDEX_DTW/queryTime" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_GRID_INDEX_DTW/lookupTime" + measureType.toString() + k, lookupTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_GRID_INDEX_DTW/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_GRID_INDEX_DTW/scannedCandidateNumberList_" + measureType.toString() + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfRealDistanceVaryingK");
    }

    private void efficiencyOfHausdorffVaryingK(Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMPoint> allPointMap, MeasureType measureType, List<Trajectory> queryList) {
        logger.info("Enter efficiencyOfRealDistanceVaryingK - " + measureType);
        for (int k = 5; k <= 50; k += 15) {
            if (k == 50) k = 39;
            logger.info("***************k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Long> lookupTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            int process = 0;
            for (Trajectory trajectory : queryList) {
                process++;
                if (process > 860 || process < 800) continue;
                //cache the results
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", candidate number: " + candidateNumberList.get(candidateNumberList.size() - 1) + ", scannedCandidate number: " + scannedCandidateNumberList.get(scannedCandidateNumberList.size() - 1) + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                nodeGridIndex.findTopK(trajectoryMap, allPointMap, trajectory, k, measureType, candidateNumberList, scannedCandidateNumberList, lookupTime);
                //complexRoadGridIndex.findTopK(trajectoryMap, allPointMap, trajectory, k, measureType, null, null, null);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("efficiencyOfHausdorffVaryingK/lookupTime" + measureType.toString() + k, lookupTime, false);
                fileManager.writeToFile("efficiencyOfHausdorffVaryingK/queryTime" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("efficiencyOfHausdorffVaryingK/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
                fileManager.writeToFile("efficiencyOfHausdorffVaryingK/scannedCandidateNumberList_" + measureType.toString() + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (k == 39) break;
        }
        logger.info("Exit efficiencyOfLORSVaryingK");
    }

    /**
     * increasing the length of query, for LCSS and EDR using FTSE
     */
    private void efficiencyOfFTSEVaryingLengthOfQuery(MeasureType measureType, Map<Integer, List<Trajectory>> queryMap, Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter efficiencyOfFTSEVaryingLengthOfQuery - {}", measureType.toString());
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            for (Trajectory trajectory : queryList) {
                Long startTime = System.nanoTime();
                FTSE.findTopK(trajectoryMap, trajectory, 15, measureType, candidateNumberList);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_FTSE/queryTime" + measureType.toString() + i, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_FTSE/candidateNumberList_" + measureType.toString() + i, candidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfFTSEVaryingLengthOfQuery - {}", measureType.toString());
    }

    /**
     * increasing the length of query, for LCSS and EDR using LEVI
     */
    private void efficiencyOfLEVIVaryingLengthOfQuery(MeasureType measureType, Map<Integer, List<Trajectory>> queryMap, Map<Integer, Short> trajLenMap, boolean compressed) {
        logger.info("Enter efficiencyOfLEVIVaryingLengthOfQuery - {}", measureType.toString());
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                //cache the result
                Long startTime = System.nanoTime();
                //nodeIndex.findTopK(complexRoadGridIndex, trajectory, 15, measureType, trajLenMap, null, null, null);
                nodeInvertedIndex.findTopK(nodeGridIndex, trajectory, 15, measureType, trajLenMap, candidateNumberList, scannedCandidateNumberList, lookupTime, compressedTime);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                String suffix = "";
                if (compressed) {
                    suffix = "_COMPRESSED";
                    fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LEVI" + suffix + "/compressedTime_" + measureType.toString() + i, compressedTime, false);
                }
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LEVI" + suffix + "/queryTime_" + measureType.toString() + i, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LEVI" + suffix + "/lookupTime_" + measureType.toString() + i, lookupTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LEVI" + suffix + "/candidateNumberList_" + measureType.toString() + i, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LEVI" + suffix + "/scannedCandidateNumber_" + measureType.toString() + i, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfLEVIVaryingLengthOfQuery - {}", measureType.toString());
    }

    /**
     * increasing the length of query, for LCSS and EDR using LEVI
     */
    private void efficiencyOfLORSVaryingLengthOfQuery(Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdges, boolean compressed) {
        logger.info("Enter efficiencyOfLORSVaryingLengthOfQuery ");
        int k = 15;
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Long> lookupTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            List<Integer> fullyScanNumList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() == QUERY_THRESHOLD) break;
                //cache the results
                List<MMEdge> originalSegments = trajectory.getMapMatchedTrajectory(allEdges);
                List<Edge> querySegments = new ArrayList<>();
                for (MMEdge originalSegment : originalSegments) {
                    querySegments.add(originalSegment);
                }
                double[] restDistance = new double[querySegments.size()];
                for (int j = querySegments.size() - 2; j >= 0 && j + 1 < querySegments.size(); --j) {
                    restDistance[j] = restDistance[j + 1] + querySegments.get(j + 1).getLength();
                }
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false, compressedTime);
                edgeInvertedIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, false, fullyScanNumList, compressedTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                if (runningTime >= 10000) {
                    Transformation.printPoints(trajectory.getMMPoints());
                    //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, null, true);
                    logger.error("trajectory hash({}) is too long", trajectory.getId());
                }
                queryTime.add(runningTime);
            }
            try {
                String suffix = "";
                if (compressed) {
                    suffix = "_COMPRESSED";
                    fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/compressedTime_" + i, compressedTime, false);
                }
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/fullyScanNumList_" + i, fullyScanNumList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/queryTime_" + i, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/lookupTime_" + i, lookupTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/candidateNumberList_" + i, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_LORS" + suffix + "/scannedCandidateNumber_" + i, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfLORSVaryingLengthOfQuery -");
    }

    private void efficiencyOfDTWVaryingLengthOfQuery(Map<Integer, Trajectory> trajectoryMap, MeasureType measureType, Map<Integer, List<Trajectory>> queryMap) {
        logger.info("Enter efficiencyOfDTWVaryingLengthOfQuery - {}", measureType.toString());
        int k = 15;
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            for (Trajectory trajectory : queryList) {
                Long startTime = System.nanoTime();
                gridIndex.findTopK(trajectoryMap, trajectory, k, measureType, candidateNumberList, scannedCandidateNumberList, null);
                gridIndex.findTopK(trajectoryMap, trajectory, k, measureType, null, null, lookupTime);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 2000000L);
            }
            try {
                fileManager.writeToFile("efficiencyOfDTWVaryingLengthOfQuery/queryTime_" + measureType.toString() + i, queryTime, false);
                fileManager.writeToFile("efficiencyOfDTWVaryingLengthOfQuery/lookupTime_" + measureType.toString() + i, lookupTime, false);
                fileManager.writeToFile("efficiencyOfDTWVaryingLengthOfQuery/candidateNumberList_" + measureType.toString() + i, candidateNumberList, false);
                fileManager.writeToFile("efficiencyOfDTWVaryingLengthOfQuery/scannedCandidateNumber_" + measureType.toString() + i, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfDTWVaryingLengthOfQuery - {}", measureType.toString());
    }

    private void efficiencyOfRealDistanceVaryingLengthOfQuery(Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMPoint> allPointMap, MeasureType measureType, Map<Integer, List<Trajectory>> queryMap) {
        logger.info("Enter efficiencyOfRealDistanceVaryingLengthOfQuery - {}", measureType.toString());
        int k = 15;
        for (int i : queryMap.keySet()) {
            logger.info("i = {}", i);
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            int process = 0;
            for (Trajectory trajectory : queryList) {
                process++;
                if (process > 200 && process < 800) continue;
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", candidate number: " + candidateNumberList.get(candidateNumberList.size() - 1) + ", scannedCandidate number: " + scannedCandidateNumberList.get(scannedCandidateNumberList.size() - 1) + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                //cache the result
                Long startTime = System.nanoTime();
                nodeGridIndex.findTopK(trajectoryMap, allPointMap, trajectory, k, measureType, candidateNumberList, scannedCandidateNumberList, lookupTime);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingLengthOfQuery/queryTime_" + measureType.toString() + i, queryTime, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingLengthOfQuery/lookupTime_" + measureType.toString() + i, lookupTime, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingLengthOfQuery/candidateNumberList_" + measureType.toString() + i, candidateNumberList, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingLengthOfQuery/scannedCandidateNumber_" + measureType.toString() + i, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfRealDistanceVaryingLengthOfQuery - {}", measureType.toString());
    }

    private int bucketKey(int key) {
        if (key <= 10) return 0;
        else if (key <= 20) return 1;
        else if (key <= 30) return 2;
        else if (key <= 40) return 3;
        else if (key <= 50) return 4;
        else if (key <= 60) return 5;
        else if (key <= 70) return 6;
        else if (key <= 80) return 7;
        else if (key <= 90) return 8;
        else if (key <= 100) return 9;
        else if (key <= 200) return 10;
        else if (key <= 300) return 11;
        else if (key <= 400) return 12;
        else if (key <= 500) return 13;
        else if (key <= 1000) return 14;
        else return 15;
    }



    public static Map<Integer, List<Integer>> readQueryFile() {
        Map<Integer, List<Integer>> idMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(EFFICIENCY_QUERY_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] lineArray = line.split(Common.instance.SEPARATOR2);
                int key = Integer.parseInt(lineArray[0]);
                List<Integer> idList = new ArrayList<>();
                idMap.put(key, idList);
                for (int i = 1; i < lineArray.length; ++i) {
                    idList.add(Integer.parseInt(lineArray[i]));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return idMap;
    }

    /**
     * verify the efficiency of LCSS and EDR using FTSE
     */
    public void efficiencyOfFTSE() {
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
        Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
        FTSE.init(trajectoryList, 10);
        List<Trajectory> queryList = loadQueryMap(false).get(2);
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(false);
        efficiencyOfFTSEVaryingK(MeasureType.LCSS, queryList, trajectoryMap);
        efficiencyOfFTSEVaryingK(MeasureType.EDR, queryList, trajectoryMap);
        efficiencyOfFTSEVaryingLengthOfQuery(MeasureType.LCSS, queryMap, trajectoryMap);
        efficiencyOfFTSEVaryingLengthOfQuery(MeasureType.EDR, queryMap, trajectoryMap);
    }

    public static void getGraphMap(Map<Integer, MMEdge> allEdgeMap, Map<Integer, MMPoint> allPointMap) {
        List<MMPoint> towerPoints = new ArrayList<>();
        List<MMEdge> allEdges = new ArrayList<>();
        Dataset.getPortoGraph(towerPoints, allEdges);
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
     * after calibration, getMMPoints() can get calibrated points
     *
     * @param trajectoryMap
     * @param allEdgeMap
     */
    public static void calibrateTrajectoryMap(Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMEdge> allEdgeMap) {
        logger.info("start to calibrate trajectories");
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        //after calibrate(), getMMPoints() can get calibrated points
        Iterator<Trajectory> iter = trajectoryList.iterator();
        while (iter.hasNext()) {
            Trajectory trajectory = iter.next();
            trajectory.getMapMatchedTrajectory(allEdgeMap);
            trajectory.calibrate();
        }
    }


    /**
     * verify the efficiency of DTW based on R Tree without map matching
     */
    private void efficiencyOfDTWBasedOnRTree() {
        logger.info("Enter efficiencyOfDTWBasedOnRTree");
        //read trajectories
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
        //buildTorGraph R tree
        envelopeIndex.buildRTree(trajectoryMap);
        if (envelopeIndex != null) return;
        //read query
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(false);
        List<Trajectory> queryList = queryMap.get(2);

        //do the experiments with varying k, for each query point, threshold algorithm
        for (int k = 5; k <= 50; k += 15) {
            logger.info("***************k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                //cache the results
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                //envelopeIndex.findTopK(trajectoryMap, trajectory, k, candidateNumberList, scannedCandidateNumberList);
                Long startTime = System.nanoTime();
                envelopeIndex.findTopK(trajectoryMap, trajectory, k, candidateNumberList, scannedCandidateNumberList);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/queryTime" + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/candidateNumberList_" + k, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/scannedCandidateNumber_" + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfDTWBasedOnRTree");
    }

    /**
     * verify the efficiency of LCSS and EDR using node dataStructure
     */
    public void efficiencyOfLEVI() {
        logger.info("Enter efficiencyOfLEVI");
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        logger.info("start to read query");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        if (!nodeInvertedIndex.load()) {
            trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
            Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
            calibrateTrajectoryMap(trajectoryMap, allEdgeMap);
//            Collection<Trajectory> extraTrajectoryList = generateExtraTrajectory();
//            for (Trajectory trajectory : extraTrajectoryList) {
//                trajectoryMap.put(trajectory.getId(), trajectory);
//            }
            nodeInvertedIndex.buildIndex(trajectoryList);
            return;
        }
        final double epsilon = 15;
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        List<Trajectory> queryList = queryMap.get(2);
        Map<Integer, Short> trajLenMap = loadCalibratedTrajectoryLengthMap();
        efficiencyOfLEVIVaryingK(MeasureType.LCSS, queryList, trajLenMap, false);
        efficiencyOfLEVIVaryingK(MeasureType.EDR, queryList, trajLenMap, false);
        efficiencyOfLEVIVaryingLengthOfQuery(MeasureType.LCSS, queryMap, trajLenMap, false);
        efficiencyOfLEVIVaryingLengthOfQuery(MeasureType.EDR, queryMap, trajLenMap, false);
        logger.info("Exit efficiencyOfLEVI");
    }

    private Collection<Trajectory> generateExtraTrajectory() {
        Collection<Trajectory> extraTrajectoryList = new ArrayList<>();
        File file = new File("EXT_TRAJECTORY_LIST");
        if (file.exists()) {
            logger.info("{} exist.", "EXT_TRAJECTORY_LIST");
            try (BufferedReader reader = new BufferedReader(new FileReader("EXT_TRAJECTORY_LIST"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] array = line.split("\t");
                    Trajectory trajectory = new Trajectory(Integer.parseInt(array[0]), array[1], null);
                    trajectory.getPoints();
                    trajectory.getMMPoints();
                    extraTrajectoryList.add(trajectory);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            logger.info("{} doesn't exist.", "EXT_TRAJECTORY_LIST");
            Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
            int id = 2100000;
            List<Trajectory> trajectoryList = queryMap.get(2);
            mapper.readPBF(environment.getProperty("PORTO_PBF_FILE_PATH"), "./target/mapmatchingtest");
            for (Trajectory trajectory : trajectoryList) {
                if (trajectory.getMMPoints().size() < 2)
                    continue;
                extraTrajectoryList.add(new Trajectory(id++, trajectory.getMMPoints()));
                for (double errorSigma = 5; errorSigma <= 10.6; errorSigma += 0.4) {
                    List<MMPoint> transformedTrajectory = transformation.randomShift(trajectory.getMMPoints(), errorSigma);
                    List<MMPoint> pointList = new ArrayList<>();
                    try {
                        mapper.fastMatchMMPoint(transformedTrajectory, pointList, new ArrayList<>());
                    } catch (Exception e) {
                        pointList = trajectory.getMMPoints();
                        e.printStackTrace();
                    }
                    extraTrajectoryList.add(new Trajectory(id++, pointList));
                }
                for (int samplingRate = 26; samplingRate <= 110; samplingRate += 6) {
                    List<MMPoint> transformedTrajectory = transformation.reSampling(trajectory.getMMPoints(), samplingRate);
                    List<MMPoint> pointList = new ArrayList<>();
                    try {
                        mapper.fastMatchMMPoint(transformedTrajectory, pointList, new ArrayList<>());
                    } catch (Exception e) {
                        pointList = trajectory.getMMPoints();
                        e.printStackTrace();
                    }
                    extraTrajectoryList.add(new Trajectory(id++, pointList));
                }
                for (double distance = 10; distance <= 38; distance += 2) {
                    List<MMPoint> transformedTrajectory = transformation.shifting(trajectory.getMMPoints(), distance);
                    List<MMPoint> pointList = new ArrayList<>();
                    try {
                        mapper.fastMatchMMPoint(transformedTrajectory, pointList, new ArrayList<>());
                    } catch (Exception e) {
                        pointList = trajectory.getMMPoints();
                        e.printStackTrace();
                    }
                    extraTrajectoryList.add(new Trajectory(id++, pointList));
                }
            }
            logger.info("trajectory size: {}", extraTrajectoryList.size());
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (Trajectory trajectory : extraTrajectoryList) {
                    writer.write(trajectory.getId() + "\t" + Trajectory.convertToPointStr(trajectory.getMMPoints()));
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return extraTrajectoryList;
    }

    /**
     * verify the efficiency of edge based method
     */
    public void efficiencyOfLORS() {
        logger.info("Enter verifyLORS");
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        if (!edgeInvertedIndex.load()) {
            trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
            Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
            calibrateTrajectoryMap(trajectoryMap, allEdgeMap);
            edgeInvertedIndex.buildIndex(trajectoryList, allEdgeMap);
            return;
        }
        logger.info("start to read query");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("start the experiments");
        efficiencyOfLORSVaryingK(queryList, allEdgeMap, false);
        efficiencyOfLORSVaryingLengthOfQuery(queryMap, allEdgeMap, false);
        logger.info("Exit verifyLORS");
    }

    /**
     * verify the efficiency of edge based method
     */
    public void efficiencyOfCompressedLORS() {
        logger.info("Enter efficiencyOfCompressedLORS");
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        edgeInvertedIndex.loadCompressedForm();
        logger.info("start to read query");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("start the experiments");
        //efficiencyOfLORSVaryingK(queryList, allEdgeMap, true);
        efficiencyOfLORSVaryingLengthOfQuery(queryMap, allEdgeMap, true);
        logger.info("Exit efficiencyOfCompressedLORS");
    }

    /**
     * verify the efficiency of edge based method
     */
    public void efficiencyOfCompressedLEVI() {
        logger.info("Enter efficiencyOfCompressedLEVI");
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        nodeInvertedIndex.loadCompressedForm();
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("start the experiments");
        final double epsilon = 15;
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        Map<Integer, Short> trajLenMap = loadCalibratedTrajectoryLengthMap();
        efficiencyOfLEVIVaryingK(MeasureType.LCSS, queryList, trajLenMap, true);
        efficiencyOfLEVIVaryingK(MeasureType.EDR, queryList, trajLenMap, true);
        efficiencyOfLEVIVaryingLengthOfQuery(MeasureType.LCSS, queryMap, trajLenMap, true);
        efficiencyOfLEVIVaryingLengthOfQuery(MeasureType.EDR, queryMap, trajLenMap, true);
        logger.info("Exit efficiencyOfCompressedLEVI");
    }

    /**
     * after map matching, verify the efficiency of DTW using grid dataStructure
     */
    private void efficiencyOfDTW() {
        logger.info("Enter efficiencyOfDTWOnGridIndex");
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        trajectoryMap = new HashMap<>();
        //read trajectory list
        logger.info("load trajectory");
        try (BufferedReader reader = new BufferedReader(new FileReader("porto_trajectory_point.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] array = line.split("\t");
                int id = Integer.parseInt(array[0]);
                String[] idArray = array[1].split(",");
                List<MMPoint> pointList = new ArrayList<>();
                for (int i = 0; i < idArray.length; i++) {
                    pointList.add(allPointMap.get(Integer.parseInt(idArray[i])));
                }
                Trajectory trajectory = new Trajectory(id, pointList);
                trajectoryMap.put(id, trajectory);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
        final double epsilon = 20.0;
        //buildTorGraph grid dataStructure
        if (!gridIndex.load()) {
            logger.info("grid dataStructure does not exist, start to buildTorGraph, trajectory size: {}", trajectoryList.size());
            List<GridIndexPoint> pointList = new LinkedList<>();
            for (Trajectory trajectory : trajectoryList) {
                Short pos = 1;
                if (trajectory.getId() % 50000 == 0)
                    logger.info("trajectory ID: {}", trajectory.getId());
                List<GridIndexPoint> list = new LinkedList<>();
                boolean valid = true;
                for (MMPoint point : trajectory.getMMPoints()) {
                    list.add(new GridIndexPoint((float) point.getLat(), (float) point.getLon(), trajectory.getId(), pos++));
                    if (point.getLat() < minLat || point.getLat() > maxLat || point.getLon() < minLon || point.getLon() > maxLon) {
                        valid = false;
                        break;
                    }
                }
                if (valid && trajectory.getPoints().size() < 1000) {
                    pointList.addAll(list);
                }
            }
            gridIndex.buildIndex(pointList, (float) epsilon);
        }
        //read query
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        efficiencyOfDTWVaryingK(trajectoryMap, MeasureType.DTW, queryList);
        efficiencyOfDTWVaryingLengthOfQuery(trajectoryMap, MeasureType.DTW, queryMap);
        logger.info("Exit efficiencyOfDTWOnGridIndex");
    }

    /**
     * calculate Hausdorff and Frechet distance after map matching using grid dataStructure
     */
    public void efficiencyOfRealDistance() {
        logger.info("Enter efficiencyOfRealDistance");
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        trajectoryMap = new HashMap<>();
        //read trajectory list
        logger.info("load trajectory - Please wait for 2 min");
        try (BufferedReader reader = new BufferedReader(new FileReader("porto_trajectory_point.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] array = line.split("\t");
                int id = Integer.parseInt(array[0]);
                String[] idArray = array[1].split(",");
                List<MMPoint> pointList = new ArrayList<>(1);
                for (int i = 0; i < idArray.length; i++) {
                    pointList.add(allPointMap.get(Integer.parseInt(idArray[i])));
                }
                Trajectory trajectory = new Trajectory(id, pointList);
                trajectoryMap.put(id, trajectory);
            }
            logger.info("load extra trajectories");
            Collection<Trajectory> extTrajList = generateExtraTrajectory();
            for (Trajectory trajectory : extTrajList) {
                trajectoryMap.put(trajectory.getId(), trajectory);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("load complete");
        Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
        final double epsilon = 50;
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            logger.info("grid dataStructure does not exist, start to buildTorGraph, trajectory size: {}", trajectoryList.size());
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        logger.info("start to read queries");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        List<Trajectory> queryList = queryMap.get(2);
        //efficiencyOfHausdorffVaryingK(trajectoryMap, allPointMap, MeasureType.DTW, queryList);
        // efficiencyOfRealDistanceVaryingLengthOfQuery(trajectoryMap, allPointMap, MeasureType.DTW, queryMap);
        efficiencyOfHausdorffVaryingK(trajectoryMap, allPointMap, MeasureType.Hausdorff, queryList);
        //efficiencyOfRealDistanceVaryingLengthOfQuery(trajectoryMap, allPointMap, MeasureType.Hausdorff, queryMap);
        efficiencyOfHausdorffVaryingK(trajectoryMap, allPointMap, MeasureType.Frechet, queryList);
        efficiencyOfRealDistanceVaryingLengthOfQuery(trajectoryMap, allPointMap, MeasureType.Frechet, queryMap);
        logger.info("Exit efficiencyOfRealDistance");
    }

    private Map<Integer, Short> loadCalibratedTrajectoryLengthMap() {

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

    private Map<Integer, Short> loadRawTrajectoryLengthMap() {
        Map<Integer, Short> res = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("RAW_TRAJECTORY_LENGTH"))) {
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

    private void deleteFile(String name) {
        try {
            Files.deleteIfExists(Paths.get(name));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * key for number of points, 0-(0,20],1-(20,40],2-(40,60],3-(60,80],4-(80,100] (10,30,50,70,90)
     * value for transformed queries
     *
     * @return
     */
    private Map<Integer, List<Trajectory>> loadQueryMap(boolean calibrated) {
        String trajectoryFile = "porto_1000query.trajectory";
        Map<Integer, List<Trajectory>> res = new HashMap<>();
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile(trajectoryFile, 10000000);
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        if (calibrated) {
            Iterator<Trajectory> iter = trajectoryMap.values().iterator();
            while (iter.hasNext()) {
                Trajectory trajectory = iter.next();
                trajectory.getMapMatchedTrajectory(allEdgeMap);
                trajectory.calibrate();
            }
        }
        for (Trajectory trajectory : trajectoryMap.values()) {
            for (int i = 10; i <= 100; i += 20) {
                StringBuilder edgeStr = new StringBuilder();
                String[] edgeArray = trajectory.getEdgeStr().split(",");
                List<MMPoint> points = new ArrayList<>();
                for (int n = 0; n < trajectory.getMMPoints().size() * i / 100; ++n) {
                    points.add(trajectory.getMMPoints().get(n));
                }
                for (int n = 0; n < edgeArray.length * i / 100; ++n) {
                    if (edgeStr.length() > 0)
                        edgeStr.append(",");
                    edgeStr.append(edgeArray[n]);
                }
                int key = (i - 1) / 20;
                List<Trajectory> trajList = res.get(key);
                if (trajList == null) {
                    trajList = new ArrayList<>();
                    res.put(key, trajList);
                }
                trajList.add(new Trajectory(trajectory.getId(), points, edgeStr.toString()));
            }
        }
        return res;
    }

    private void convertQueriesToTenForms() {
        String trajectoryFile = "porto_1000query.trajectory";
        mapper.readPBF(environment.getProperty("PORTO_PBF_FILE_PATH"), "./target/mapmatchingtest");
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile(trajectoryFile, 10000000);
        for (int i = 10; i <= 100; i += 10)
            deleteFile("porto_1000query_length" + i + ".trajectory");
        for (Trajectory trajectory : trajectoryMap.values()) {
            List<MMPoint> points = new ArrayList<>();
            for (int i = 1; i <= 100; ++i) {
                points.add(trajectory.getMMPoints().get(i - 1));
                if (i % 10 == 0) {
                    List<MMPoint> pathPoints = new ArrayList<>();
                    List<MMEdge> edges = new ArrayList<>();
                    mapper.fastMatchMMPoint(points, pathPoints, edges);
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter("porto_1000query_length" + i + ".trajectory", true))) {
                        writer.write(trajectory.getId() + "\t");
                        writer.write(Trajectory.convertToPointStr(trajectory.getMMPoints()) + "\t");
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
    }

    /**
     * This is queries of length 100
     *
     * @return
     */
    private List<Trajectory> loadQueryList(double ratio) {

        return null;
    }

    public void reChooseQueries() {
        logger.info("Enter reChooseQueries");
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        //key for the number of points, value for hash list
        List<Integer> idList = new ArrayList<>();
        for (Trajectory trajectory : trajectoryList) {
            if (trajectory.getPoints().size() == 100 && trajectory.isValidQuery())
                idList.add(trajectory.getId());
        }
        logger.info("valid trajectory size: {}", idList.size());

        //randomly pick 1000 queries with length of 100
        SecureRandom random = new SecureRandom();
        Set<Integer> luckyIDSet = new HashSet<>();
        while (luckyIDSet.size() < idList.size() && luckyIDSet.size() < 1000) {
            luckyIDSet.add(idList.get(random.nextInt(idList.size())));
        }

        //save files
        List<Integer> sortedIDList = new ArrayList<>(luckyIDSet);
        sortedIDList.sort(Comparator.naturalOrder());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("porto_1000query.trajectory"))) {
            for (Integer id : luckyIDSet) {
                Trajectory trajectory = trajectoryMap.get(id);
                writer.write(trajectory.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void compressIndexFile() {
        //edgeIndex.compress(false);
        nodeInvertedIndex.compress(false);
        //edgeIndex.loadCompressedForm();
    }

    private void writeInt(int n, ByteBuffer buffer, OutputStream writer) throws IOException {
        buffer.putInt(n);
        writer.write(buffer.array());
        buffer.clear();
    }

    private void createInvertedIndex() {
        //key for point hash, value for trajectory hash and frequency
        Map<Integer, Map<Integer, Integer>> invertedIndex = new HashMap<>();
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        trajectoryMap = new HashMap<>();
        //read trajectory list
        logger.info("load trajectory");
        try (BufferedReader reader = new BufferedReader(new FileReader("porto_trajectory_point.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] array = line.split("\t");
                int id = Integer.parseInt(array[0]);
                String[] idArray = array[1].split(",");
                List<MMPoint> pointList = new ArrayList<>(idArray.length);
                for (int i = 0; i < idArray.length; i++) {
                    pointList.add(allPointMap.get(Integer.parseInt(idArray[i])));
                }
                Trajectory trajectory = new Trajectory(id, pointList);
                trajectoryMap.put(id, trajectory);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("trajectory size: {}", trajectoryMap.size());
        logger.info("reordering point hash");
        int idSeq = 0;
        for (Map.Entry<Integer, MMPoint> pointEntry : allPointMap.entrySet()) {
            pointEntry.getValue().setId(idSeq++);
        }
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        for (Trajectory trajectory : trajectoryList) {
            for (MMPoint point : trajectory.getMMPoints()) {
                Map<Integer, Integer> postingList = invertedIndex.get(point.getId());
                if (postingList == null) {
                    postingList = new HashMap<>();
                    invertedIndex.put(point.getId(), postingList);
                }
                Integer frequency = postingList.get(trajectory.getId());
                if (frequency == null) {
                    postingList.put(trajectory.getId(), 1);
                } else {
                    postingList.put(trajectory.getId(), frequency + 1);
                }
            }
        }
        logger.info("write to file");
        try (OutputStream docWriter = new FileOutputStream("porto.docs");
             OutputStream freqWriter = new FileOutputStream("porto.freqs");
             OutputStream sizeWriter = new FileOutputStream("porto.sizes")) {
            ByteBuffer buffer = ByteBuffer.allocate(4);
            //docs and freqs
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            writeInt(1, buffer, docWriter);
            writeInt(trajectoryList.size(), buffer, docWriter);
            for (Map.Entry<Integer, Map<Integer, Integer>> entry : invertedIndex.entrySet()) {
                int pointID = entry.getKey();
                Map<Integer, Integer> postingList = invertedIndex.get(pointID);
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

    public void generateWhetherIntersectIndex() {
        logger.info("Enter generateWhetherIntersectIndex");
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        if (!edgeInvertedIndex.load()) {
            Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
            calibrateTrajectoryMap(trajectoryMap, allEdgeMap);
            edgeInvertedIndex.buildIndex(trajectoryList, allEdgeMap);
        }
        //key is trajectory hash, value is relevant trajectory hash list
        Map<Integer, Set<Integer>> results = new ConcurrentHashMap<>();
        ExecutorService threadPool = new ThreadPoolExecutor(40, 40, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0);
        List<Trajectory> queryList = new ArrayList<>(1000);
        try {
            Files.lines(Paths.get("mm.lcss.top100")).forEach(line -> {
                Trajectory trajectory = trajectoryMap.get(Integer.parseInt(line.split(" ")[0]));
                if (trajectory != null)
                    queryList.add(trajectory);
            });
        } catch (IOException e) {
            logger.error("{}", e);
        }
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
                        Set<Integer> relevantIDSet = results.get(trajectory.getId());
                        if (relevantIDSet == null) {
                            relevantIDSet = new HashSet<>();
                            results.put(trajectory.getId(), relevantIDSet);
                        }
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
        logger.info("Exit generateWhetherIntersectIndex");
    }

    public void testMapMatchingTime() {
        logger.info("Enter");
        mapper.readPBF(environment.getProperty("PORTO_PBF_FILE_PATH"), "./target/mapmatchingtest");
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory.txt", 10000000);
        for (Trajectory trajectory : trajectoryMap.values()) {
            trajectory.getMMPoints();
        }
        logger.info("Start - trajectory size: {}", trajectoryMap.size());
        int n = 0;
        long startDate = System.nanoTime();
        for (Trajectory trajectory : trajectoryMap.values()) {
            if (trajectory.getId() % 1000 == 0)
                System.out.println(trajectory.getId());
            List<MMEdge> edges = new ArrayList<>();
            List<MMPoint> points = new ArrayList<>();
            try {
                mapper.fastMatchMMPoint(trajectory.getMMPoints(), points, edges);
            } catch (Exception e) {
                ++n;
            }
        }
        System.out.println(n);
        logger.info("elapsed time: {}", (System.nanoTime() - startDate) / 1000000L);
    }

    public void efficiencyOfRTreeBasedRangeQuery() {
        logger.info("Enter efficiencyOfRTreeBasedRangeQuery");
        logger.info("Enter efficiencyOfDTWBasedOnRTree");
        //read trajectories
        Map<Integer, Trajectory> trajectoryMap = Dataset.loadMysqlTrajTxtFile("porto_trajectory2.txt", 10000000);
        //buildTorGraph R tree
        envelopeIndex.buildRTree(trajectoryMap);
        List<MMPoint> queryPointList = Arrays.asList(new MMPoint(41.157906, -8.627803),
                new MMPoint(41.159450, -8.643841),
                new MMPoint(41.164406, -8.655273),
                new MMPoint(41.166788, -8.647873),
                new MMPoint(41.168863, -8.629041),
                new MMPoint(41.165021, -8.618527),
                new MMPoint(41.154094, -8.606545),
                new MMPoint(41.148859, -8.638131),
                new MMPoint(41.151574, -8.625514),
                new MMPoint(41.148730, -8.640534));

        double deltaR = 50;
        List<Long> queryTime = new ArrayList<>();
        for (double r = 100; r <= 500; r += deltaR) {
            long qTime = 0;
            for (MMPoint point : queryPointList) {
                long startTime = System.nanoTime();
                envelopeIndex.rangeQuery(point, r);
                qTime += (System.nanoTime() - startTime) / 1000000L;
            }
            queryTime.add(qTime / queryPointList.size());
        }
        try {
            fileManager.writeToFile("EFFICIENCY_RANGE_QUERY_RTree" + "/queryTime_", queryTime, false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Exit efficiencyOfRTreeBasedRangeQuery");
    }

    public void efficiencyOfRangeQuery() {
        logger.info("Enter efficiencyOfGridBasedRangeQuery");
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        final double epsilon = 15;
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        nodeInvertedIndex.loadCompressedForm();
        List<MMPoint> queryPointList = Arrays.asList(new MMPoint(41.157906, -8.627803),
                new MMPoint(41.159450, -8.643841),
                new MMPoint(41.164406, -8.655273),
                new MMPoint(41.166788, -8.647873),
                new MMPoint(41.168863, -8.629041),
                new MMPoint(41.165021, -8.618527),
                new MMPoint(41.154094, -8.606545),
                new MMPoint(41.148859, -8.638131),
                new MMPoint(41.151574, -8.625514),
                new MMPoint(41.148730, -8.640534));

        double deltaR = 50;
        List<Long> queryTime = new ArrayList<>();
        List<Long> compressedTime = new ArrayList<>();
        for (double r = 100; r <= 500; r += deltaR) {
            long qTime = 0, cTime = 0;
            for (MMPoint point : queryPointList) {
                List<Long> tempList = new ArrayList<>();
                long startTime = System.nanoTime();
                nodeInvertedIndex.rangeQuery(nodeGridIndex, point, r, tempList);
                qTime += (System.nanoTime() - startTime) / 1000000L;
                cTime += tempList.get(0);
                System.out.println((System.nanoTime() - startTime));
            }
            queryTime.add(qTime / queryPointList.size());
            compressedTime.add(cTime / queryPointList.size());
        }
        try {
            fileManager.writeToFile("EFFICIENCY_RANGE_QUERY_LEVI" + "/compressedTime", compressedTime, false);
            fileManager.writeToFile("EFFICIENCY_RANGE_QUERY_LEVI" + "/queryTime", queryTime, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit efficiencyOfGridBasedRangeQuery");
    }

    public void efficiencyOfPathQuery() {
        logger.info("Enter efficiencyOfPathQuery");
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        edgeInvertedIndex.loadCompressedForm();
        logger.info("start to read query");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        logger.info("start experiments");
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Long> queryTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() == QUERY_THRESHOLD) break;
                List<MMEdge> originalSegments = trajectory.getMapMatchedTrajectory(allEdgeMap);
                long startTime = System.nanoTime();
                edgeInvertedIndex.pathQuery(originalSegments, compressedTime);
                queryTime.add((System.nanoTime() - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_PATH_QUERY_LEVI" + "/compressedTime_" + i, compressedTime, false);
                fileManager.writeToFile("EFFICIENCY_PATH_QUERY_LEVI" + "/queryTime_" + i, queryTime, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Enter efficiencyOfPathQuery");
    }

    public void efficiencyOfStrictPathQuery() {
        logger.info("Enter efficiencyOfStrictPathQuery");
        Map<Integer, Trajectory> trajectoryMap = null;
        Map<Integer, MMEdge> allEdgeMap = new HashMap<>();
        Map<Integer, MMPoint> allPointMap = new HashMap<>();
        getGraphMap(allEdgeMap, allPointMap);
        edgeInvertedIndex.loadCompressedForm();
        logger.info("start to read query");
        Map<Integer, List<Trajectory>> queryMap = loadQueryMap(true);
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Long> queryTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() == QUERY_THRESHOLD) break;
                if (queryTime.size() > 0)
                    logger.info("{}, query time: {}", queryTime.size(), queryTime.get(queryTime.size() - 1));
                List<MMEdge> originalSegments = trajectory.getMapMatchedTrajectory(allEdgeMap);
                long startTime = System.nanoTime();
                edgeInvertedIndex.strictPathQuery(originalSegments, compressedTime);
                queryTime.add((System.nanoTime() - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_STRICT_PATH_QUERY_LEVI" + "/compressedTime_" + i, compressedTime, false);
                fileManager.writeToFile("EFFICIENCY_STRICT_PATH_QUERY_LEVI" + "/queryTime_" + i, queryTime, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Enter efficiencyOfStrictPathQuery");
    }

    //@PostConstruct
    public void run() {

        efficiencyOfStrictPathQuery();

        efficiencyOfLEVI();
        generateWhetherIntersectIndex();
        loadQueryMap(true);
        reChooseQueries();
        //generateEfficiencyIDs();
        efficiencyOfFTSE();
        efficiencyOfCompressedLORS();
        efficiencyOfLEVI();

        efficiencyOfCompressedLEVI();
        efficiencyOfLORS();
        efficiencyOfDTWBasedOnRTree();
        efficiencyOfDTW();
        createInvertedIndex();
        efficiencyOfRealDistance();
        compressIndexFile();


        logger.info("end");
    }
}
//nohup java -XX:+UseG1GC -Xmx120G -jar shareniu.jar &> nohup1.out&
//nohup java -XX:+UseG1GC -Xmx50G -jar efficiencyOfLEVI2.jar &> nohup2.out&
//nohup java -XX:+UseG1GC -Xmx100G -jar efficiencyOfFTSE.jar &> nohup2.out&
//nohup java -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -Xmx100G -jar &> FTSE.out&
//nohup java -Xmx160G -javaagent:"/home/el4/E29944/LORS_EXP/classmexer.jar" -jar