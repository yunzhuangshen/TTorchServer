package au.edu.rmit.trajectory.similarity;

import au.edu.rmit.trajectory.similarity.algorithm.FastTimeSeriesEvaluation;
import au.edu.rmit.trajectory.similarity.algorithm.Transformation;
import au.edu.rmit.trajectory.similarity.datastructure.NodeGridIndex;
import au.edu.rmit.trajectory.similarity.datastructure.EdgeInvertedIndex;
import au.edu.rmit.trajectory.similarity.datastructure.NodeInvertedIndex;
import au.edu.rmit.trajectory.torch.io.BeijingDataset;
import au.edu.rmit.trajectory.torch.io.FileManager;
import au.edu.rmit.trajectory.similarity.model.*;
import au.edu.rmit.trajectory.similarity.datastructure.RTreeWrapper;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

/**
 * The class contains functions of efficiency experiment for varies top k finding algorithm.
 *
 * @author forrest0402
 */
@Component
public class EfficiencyExp {

    private static Logger logger = LoggerFactory.getLogger(EfficiencyExp.class);

    @Autowired
    FileManager fileManager;

    /******************************
     * LORS                       *
     * ****************************/

    private void efficiencyOfLORSVaryingK(EdgeInvertedIndex edgeInvertedIndex, List<Trajectory> queryList, Map<Integer, MMEdge> allEdges, boolean compressed) {
        logger.info("Enter efficiencyOfLORSVaryingK");
        for (int k = 5; k <= 50; k += 15) {
            logger.info("***************k = {}******************", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            List<Integer> fullyScanNumList = new ArrayList<>();

            for (Trajectory queryTraj : queryList) {
                //cache the results
                List<MMEdge> originalSegments = queryTraj.getMapMatchedTrajectory(allEdges);
                List<Edge> querySegments = new ArrayList<>(originalSegments);

                double[] restDistance = new double[querySegments.size()];
                for (int i = querySegments.size() - 2; i >= 0 && i + 1 < querySegments.size(); --i) {
                    restDistance[i] = restDistance[i + 1] + querySegments.get(i + 1).getLength();
                }

                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + queryTraj.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");

                Long startTime = System.nanoTime();
                edgeInvertedIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, false, fullyScanNumList, compressedTime);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false,compressedTime);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false,compressedTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                if (runningTime >= 10000) {
                    Transformation.printPoints(queryTraj.getMMPoints());
                    //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, true);
                    logger.error("queryTraj hash({}) is too long", queryTraj.getId());
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

    private void efficiencyOfLORSVaryingLengthOfQuery(EdgeInvertedIndex edgeInvertedIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdges, boolean compressed) {
        logger.info("Enter efficiencyOfLORSVaryingLengthOfQuery ");
        final int k = 20;   //number of results

        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Long> lookupTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            List<Integer> fullyScanNumList = new ArrayList<>();
            for (Trajectory query_traj : queryList) {

                //cache the results
                List<MMEdge> originalSegments = query_traj.getMapMatchedTrajectory(allEdges);
                List<Edge> querySegments = new ArrayList<>(originalSegments);

                double[] restDistance = new double[querySegments.size()];
                for (int j = querySegments.size() - 2; j >= 0 && j + 1 < querySegments.size(); --j) {
                    restDistance[j] = restDistance[j + 1] + querySegments.get(j + 1).getLength();
                }
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + query_traj.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false);
                //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, null, null, null, false, compressedTime);
                edgeInvertedIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, lookupTime, false, fullyScanNumList, compressedTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                if (runningTime >= 10000) {
                    Transformation.printPoints(query_traj.getMMPoints());
                    //edgeIndex.findTopK(querySegments, k, allEdges, restDistance, candidateNumberList, scannedCandidateNumberList, null, true);
                    logger.error("query_traj hash({}) is too long", query_traj.getId());
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

    /*******************************
     * LCSS, EDR supported by FTSE *
     * *****************************/

    private void efficiencyOfFTSEVaryingK(FastTimeSeriesEvaluation FTSE, MeasureType measureType, List<Trajectory> queryList, Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter efficiencyOfFTSEVaryingK - {}", measureType.toString());
        for (int k = 5; k <= 50; k += 15) {
            logger.info("k = {}", k);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                List<Integer> result = FTSE.findTopK(trajectoryMap, trajectory, k, measureType, candidateNumberList);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_K_FTSE/queryTime_" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_FTSE/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfFTSEVaryingK - {}", measureType.toString());
    }

    private void efficiencyOfFTSEVaryingLengthOfQuery(FastTimeSeriesEvaluation FTSE, MeasureType measureType, Map<Integer, List<Trajectory>> queryMap, Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter efficiencyOfFTSEVaryingLengthOfQuery - {}", measureType.toString());
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            for (Trajectory trajectory : queryList) {
                Long startTime = System.nanoTime();
                FTSE.findTopK(trajectoryMap, trajectory, 20, measureType, candidateNumberList);
                Long endTime = System.nanoTime();
                queryTime.add((endTime - startTime) / 1000000L);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_FTSE/queryTime_" + measureType.toString() + i, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_QUERY_LENGTH_FTSE/candidateNumberList_" + measureType.toString() + i, candidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfFTSEVaryingLengthOfQuery - {}", measureType.toString());
    }

    /*******************************
     * LCSS, EDR backed by LEVI *
     * *****************************/

    private void efficiencyOfLEVIVaryingK(NodeInvertedIndex nodeInvertedIndex, NodeGridIndex nodeGridIndex, MeasureType measureType,
                                          List<Trajectory> queryList, Map<Integer, Short> trajLenMap, boolean compressed) {
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
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() +
                                            " -> " + queryTime.get(queryTime.size() - 1) + " ms");

                long startTime = System.nanoTime();
                nodeInvertedIndex.findTopK(nodeGridIndex, trajectory, k, measureType, trajLenMap, candidateNumberList, scannedCandidateNumberList, lookupTime, compressedTime);
                long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
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

    private void efficiencyOfLEVIVaryingLengthOfQuery(NodeInvertedIndex nodeInvertedIndex, NodeGridIndex nodeGridIndex, MeasureType measureType, Map<Integer, List<Trajectory>> queryMap, Map<Integer, Short> trajLenMap, boolean compressed) {
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
                nodeInvertedIndex.findTopK(nodeGridIndex, trajectory, 20, measureType, trajLenMap, candidateNumberList, scannedCandidateNumberList, lookupTime, compressedTime);
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

    /******************************
     * Real Distance              *
     * ****************************/

    private void efficiencyOfRealDistanceVaryingK(NodeGridIndex nodeGridIndex, Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMPoint> allPointMap, MeasureType measureType, List<Trajectory> queryList) {
        logger.info("Enter efficiencyOfRealDistanceVaryingK - " + measureType);
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
                nodeGridIndex.findTopK(trajectoryMap, allPointMap, trajectory, k, measureType, candidateNumberList, scannedCandidateNumberList, lookupTime);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingK/lookupTime_" + measureType.toString() + k, lookupTime, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingK/queryTime_" + measureType.toString() + k, queryTime, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingK/candidateNumberList_" + measureType.toString() + k, candidateNumberList, false);
                fileManager.writeToFile("efficiencyOfRealDistanceVaryingK/scannedCandidateNumber_" + measureType.toString() + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit efficiencyOfLORSVaryingK");
    }

    private void efficiencyOfRealDistanceVaryingLengthOfQuery(NodeGridIndex nodeGridIndex, Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMPoint> allPointMap, MeasureType measureType, Map<Integer, List<Trajectory>> queryMap) {
        logger.info("Enter efficiencyOfRealDistanceVaryingLengthOfQuery - {}", measureType.toString());
        int k = 20;
        for (int i : queryMap.keySet()) {
            logger.info("i = {}", i);
            List<Trajectory> queryList = queryMap.get(i);
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            List<Long> lookupTime = new ArrayList<>();
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            for (Trajectory trajectory : queryList) {
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

    /******************************
     * efficiency methods         *
     * ****************************/

    public void efficiencyOfLORS(boolean toggleoff, EdgeInvertedIndex edgeInvertedIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdgeMap) {
        if (toggleoff) return;
        if (edgeInvertedIndex == null) throw new IllegalArgumentException("edgeIndex is null");
        logger.info("Enter verifyLORS");
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("start the experiments");
        efficiencyOfLORSVaryingK(edgeInvertedIndex, queryList, allEdgeMap, false);
        efficiencyOfLORSVaryingLengthOfQuery(edgeInvertedIndex, queryMap, allEdgeMap, false);
        logger.info("Exit verifyLORS");
    }

    public void efficiencyOfLORSWithCompression(boolean toggleoff, EdgeInvertedIndex edgeInvertedIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdgeMap) {
        if (toggleoff) return;
        if (edgeInvertedIndex == null) throw new IllegalArgumentException("edgeIndex is null");
        logger.info("Enter efficiencyOfLORSWithCompression");
        List<Trajectory> queryList = queryMap.get(2);
        logger.info("start the experiments");
        efficiencyOfLORSVaryingK(edgeInvertedIndex, queryList, allEdgeMap, true);
        efficiencyOfLORSVaryingLengthOfQuery(edgeInvertedIndex, queryMap, allEdgeMap, true);
        logger.info("Exit efficiencyOfLORSWithCompression");
    }

    public void efficiencyOfFTSE(boolean toggleoff, FastTimeSeriesEvaluation FTSE, Map<Integer, List<Trajectory>> queryMap, Map<Integer, Trajectory> trajectoryMap) {
        if (toggleoff) return;
        Collection<Trajectory> trajectoryList = trajectoryMap.values(); //trajectoryService.getTrajectories(new ArrayList<>(IDs));
//        int hash = 300000;
//        for (List<Trajectory> trajectories : queryMap.values()) {
//            for (Trajectory trajectory : trajectories) {
//                trajectoryMap.put(hash++, trajectory);
//            }
//        }
        List<Trajectory> queryList = queryMap.get(2);
        FTSE.init(trajectoryList, 500);
        efficiencyOfFTSEVaryingK(FTSE, MeasureType.LCSS, queryList, trajectoryMap);
        efficiencyOfFTSEVaryingK(FTSE, MeasureType.EDR, queryList, trajectoryMap);
        efficiencyOfFTSEVaryingLengthOfQuery(FTSE, MeasureType.LCSS, queryMap, trajectoryMap);
        efficiencyOfFTSEVaryingLengthOfQuery(FTSE, MeasureType.EDR, queryMap, trajectoryMap);
    }


    /**
     * Load LEVI dataStructure and necessary data to memory. Then test the efficiency of EDR and LCSS.
     */
    public void efficiencyOfLEVI(boolean toggleoff, NodeInvertedIndex nodeInvertedIndex, NodeGridIndex nodeGridIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMPoint> allPointMap) {
        if (toggleoff) return;
        logger.info("Enter efficiencyOfLEVI");
        logger.info("start to read query");
        if (!nodeInvertedIndex.load()) {
            Map<Integer, Trajectory> trajectoryMap = BeijingDataset.loadBeijingTrajectoryTxtFile(true);
            Collection<Trajectory> trajectoryList = trajectoryMap.values();
            nodeInvertedIndex.buildIndex(trajectoryList);
            return;
        }

        // length and height for a tile.
        final double epsilon = 25;
        nodeGridIndex.delete();
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        List<Trajectory> queryList = queryMap.get(2);
        Map<Integer, Short> trajLenMap = BeijingDataset.loadCalibratedTrajectoryLengthMap();
        efficiencyOfLEVIVaryingK(nodeInvertedIndex, nodeGridIndex, MeasureType.LCSS, queryList, trajLenMap, false);
        efficiencyOfLEVIVaryingK(nodeInvertedIndex, nodeGridIndex, MeasureType.EDR, queryList, trajLenMap, false);
        efficiencyOfLEVIVaryingLengthOfQuery(nodeInvertedIndex, nodeGridIndex, MeasureType.LCSS, queryMap, trajLenMap, false);
        efficiencyOfLEVIVaryingLengthOfQuery(nodeInvertedIndex, nodeGridIndex, MeasureType.EDR, queryMap, trajLenMap, false);
        logger.info("Exit efficiencyOfLEVI");
    }

    /**
     * Load compressed LEVI dataStructure and necessary data to memory. Then test the efficiency of EDR and LCSS, backed by LEVI
     */
    public void efficiencyOfLEVIWithCompression(boolean toggleoff, NodeInvertedIndex nodeInvertedIndex, NodeGridIndex nodeGridIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMPoint> allPointMap) {
        if (toggleoff) return;
        logger.info("Enter efficiencyOfLEVIWithCompression");
        final double epsilon = 25;
        nodeGridIndex.delete();
        //buildTorGraph grid dataStructure
        if (!nodeGridIndex.load()) {
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        List<Trajectory> queryList = queryMap.get(2);
        Map<Integer, Short> trajLenMap = BeijingDataset.loadCalibratedTrajectoryLengthMap();
        efficiencyOfLEVIVaryingK(nodeInvertedIndex, nodeGridIndex, MeasureType.LCSS, queryList, trajLenMap, true);
        efficiencyOfLEVIVaryingK(nodeInvertedIndex, nodeGridIndex, MeasureType.EDR, queryList, trajLenMap, true);
        efficiencyOfLEVIVaryingLengthOfQuery(nodeInvertedIndex, nodeGridIndex, MeasureType.LCSS, queryMap, trajLenMap, true);
        efficiencyOfLEVIVaryingLengthOfQuery(nodeInvertedIndex, nodeGridIndex, MeasureType.EDR, queryMap, trajLenMap, true);
        logger.info("Exit efficiencyOfLEVIWithCompression");
    }

    /**
     * Load RTree dataStructure and necessory data to memory. Then test the efficiency of DTW
     */
    public void efficiencyOfDTWBasedOnRTree(RTreeWrapper envelopeIndex, Map<Integer, Trajectory> trajectoryMap, Map<Integer, List<Trajectory>> queryMap) {
        logger.info("Enter efficiencyOfDTWBasedOnRTree");
        //read trajectories
        //buildTorGraph R tree
        envelopeIndex.buildRTree(trajectoryMap);
        //read query
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
                Long startTime = System.nanoTime();
                envelopeIndex.findTopK(trajectoryMap, trajectory, k, candidateNumberList, scannedCandidateNumberList);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/queryTime_" + k, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/candidateNumberList_" + k, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_K_RTREE_DTW/scannedCandidateNumber_" + k, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (int i : queryMap.keySet()) {
            queryList = queryMap.get(i);
            logger.info("***************i = {}******************", i);
            List<Long> queryTime = new ArrayList<>();//unit milliseconds
            List<Integer> candidateNumberList = new ArrayList<>();
            List<Integer> scannedCandidateNumberList = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
                //cache the results
                if (queryTime.size() > 0)
                    System.out.println(queryTime.size() + ", number of points: " + trajectory.getMMPoints().size() + " -> " + queryTime.get(queryTime.size() - 1) + " ms");
                Long startTime = System.nanoTime();
                envelopeIndex.findTopK(trajectoryMap, trajectory, 20, candidateNumberList, scannedCandidateNumberList);
                Long endTime = System.nanoTime();
                long runningTime = (endTime - startTime) / 1000000L;
                queryTime.add(runningTime);
            }
            try {
                fileManager.writeToFile("EFFICIENCY_VARYING_LENGTH_RTREE_DTW/queryTime_" + i, queryTime, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_LENGTH_RTREE_DTW/candidateNumberList_" + i, candidateNumberList, false);
                fileManager.writeToFile("EFFICIENCY_VARYING_LENGTH_RTREE_DTW/scannedCandidateNumber_" + i, scannedCandidateNumberList, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        logger.info("Exit efficiencyOfDTWBasedOnRTree");
    }

    /**
     * Load Grid dataStructure and necessary data to memory. Then test the efficiency of DTW
     */
    public void efficiencyOfDTWOnGridIndex(NodeGridIndex nodeGridIndex, Map<Integer, Trajectory> trajectoryMap, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdgeMap, Map<Integer, MMPoint> allPointMap) {
        logger.info("Enter efficiencyOfDTWOnGridIndex");
        //read trajectory list
        Collection<Trajectory> trajectoryList = trajectoryMap.values();
        final double epsilon = 100;
        //buildTorGraph grid dataStructure
        nodeGridIndex.delete();
        if (!nodeGridIndex.load()) {
            logger.info("grid dataStructure does not exist, start to buildTorGraph, trajectory size: {}", trajectoryList.size());
            nodeGridIndex.buildIndex(allPointMap, (float) epsilon);
        }
        logger.info("start to read queries");
        List<Trajectory> queryList = queryMap.get(2);
        efficiencyOfRealDistanceVaryingK(nodeGridIndex, trajectoryMap, allPointMap, MeasureType.DTW, queryList);
        efficiencyOfRealDistanceVaryingLengthOfQuery(nodeGridIndex, trajectoryMap, allPointMap, MeasureType.DTW, queryMap);
        logger.info("Exit efficiencyOfDTWOnGridIndex");
    }

    /**
     * Load RTree dataStructure and necessary data to memory. Then test the efficiency of RQ
     */
    public void efficiencyOfRTreeBasedRangeQuery(RTreeWrapper envelopeIndex, Map<Integer, Trajectory> trajectoryMap) {
        logger.info("Enter efficiencyOfRTreeBasedRangeQuery");
        logger.info("Enter efficiencyOfDTWBasedOnRTree");
        //read trajectories
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
                qTime += (System.nanoTime() - startTime);
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

    /**
     * Load Grid dataStructure and necessary data to memory. Then test the efficiency of RQ
     */
    public void efficiencyOfGridBasedRangeQuery(NodeGridIndex nodeGridIndex, NodeInvertedIndex nodeInvertedIndex, Map<Integer, MMPoint> allPointMap) {
        logger.info("Enter efficiencyOfGridBasedRangeQuery");
        final double epsilon = 100;
        //buildTorGraph grid dataStructure
        nodeGridIndex.delete();
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
                qTime += (System.nanoTime() - startTime);
                cTime += tempList.get(0);
                logger.info("q time: {}, c time: {}", qTime, cTime);
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

    /**
     * Load LEVI and necessary data to memory. Then test the efficiency of RQ
     */
    public void efficiencyOfPathQuery(EdgeInvertedIndex edgeInvertedIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdgeMap) {
        logger.info("Enter efficiencyOfPathQuery");
        edgeInvertedIndex.loadCompressedForm();
        logger.info("start to read query");
        logger.info("start experiments");
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Long> queryTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
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

    /**
     * Load LEVI and necessary data to memory. Then test the efficiency of RQ
     */
    public void efficiencyOfStrictPathQuery(EdgeInvertedIndex edgeInvertedIndex, Map<Integer, List<Trajectory>> queryMap, Map<Integer, MMEdge> allEdgeMap) {
        logger.info("Enter efficiencyOfStrictPathQuery");
        edgeInvertedIndex.loadCompressedForm();
        logger.info("start to read query");
        for (int i : queryMap.keySet()) {
            List<Trajectory> queryList = queryMap.get(i);
            List<Long> queryTime = new ArrayList<>();
            List<Long> compressedTime = new ArrayList<>();
            for (Trajectory trajectory : queryList) {
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
}
