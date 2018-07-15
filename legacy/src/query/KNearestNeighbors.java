package au.edu.rmit.trajectory.similarity.query;

import au.edu.rmit.trajectory.similarity.algorithm.LongestCommonSubsequence;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.model.MapMatchedTrajectory;
import au.edu.rmit.trajectory.similarity.model.Segment;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.service.MapMatchedTrajectoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @author forrest0402
 * @Description
 * @date 11/25/2017
 */
@Component
public class KNearestNeighbors {

    private static Logger logger = LoggerFactory.getLogger(KNearestNeighbors.class);

    @Autowired
    MapMatchedTrajectoryService mapMatchedTrajectoryService;

    @Autowired
    LongestCommonSubsequence LCSS;

    List<MapMatchedTrajectory> mapMatchedTrajectoryDatabase;

    /**
     * segmentId to MapMatchedTrajectoryId
     */
    Map<Integer, Set<Integer>> segmentToMapMatchedTraj;


    public List<Integer> LongestCommonSubsequence(Trajectory trajectory, Mapper mapper, int k, MatchMode mode) {
        switch (mode) {
            case PointBased:
                break;
            case EdgeBased:
                break;
        }
        return null;
    }

    public List<Integer> DynamicTimeWarping(Trajectory trajectory, Mapper mapper, int k) {
        return null;
    }

    public List<Integer> EditDistanceOnRealSubsequence(Trajectory trajectory, Mapper mapper, int k) {
        return null;
    }

    public List<Integer> EditDistanceWithRealPenalty(Trajectory trajectory, Mapper mapper, int k) {
        return null;
    }

    public void initialize(List<MapMatchedTrajectory> mapMatchedTrajectoryDatabase) {
        this.mapMatchedTrajectoryDatabase = mapMatchedTrajectoryDatabase;
        this.segmentToMapMatchedTraj = new HashMap<>();
        for (MapMatchedTrajectory mapMatchedTrajectory : mapMatchedTrajectoryDatabase) {
            for (Segment segment : mapMatchedTrajectory.getSegments()) {
                Set<Integer> mapMatchedTrajIds = segmentToMapMatchedTraj.get(segment.getID());
                if (mapMatchedTrajIds == null) {
                    mapMatchedTrajIds = new HashSet<>();
                    segmentToMapMatchedTraj.put(segment.getID(), mapMatchedTrajIds);
                }
                mapMatchedTrajIds.add(mapMatchedTrajectory.getId());
            }
        }
    }

    private MapMatchedTrajectory findMapMatchedTrajectoryByID(int mmtID) {
        for (MapMatchedTrajectory mapMatchedTrajectory : mapMatchedTrajectoryDatabase) {
            if (mapMatchedTrajectory.getId() == mmtID)
                return mapMatchedTrajectory;
        }
        return null;
    }

    /**
     * Filter and refine, first retrieve topk elements based on inverted dataStructure, then refine the results
     *
     * @param trajectory
     * @param mapper
     * @param k
     * @return
     */
    public List<Integer> run(Trajectory trajectory, Mapper mapper, int k) {
//        logger.info("Enter KNearestNeighbors");
//        final List<Segment> querySegments = new ArrayList<>();
        List<Integer> result = new ArrayList<>(k);
////        try {
//        logger.debug("Start to map matching");
//        long beforemm = System.nanoTime();
//        Common.instance.IntBox2.add(trajectory.getPoints().size());
//        List<Segment> rawSegments = trajectoryMapping.match(trajectory.getPoints());
//        Common.instance.IntBox3.add(rawSegments.size());
//        //adjust segments
//        logger.debug("Start to adjust segments");
//        double trajLen = 0;
//        for (Segment rawSegment : rawSegments) {
//            Segment matchedSegment = Common.instance.ALL_SEGMENTS.get(Common.instance.ALL_EDGEMATCHES.get(rawSegment.getKey()));
//            querySegments.add(matchedSegment);
//            trajLen += rawSegment.getLength();
//        }
//        Common.instance.LongBox.add((System.nanoTime() - beforemm) / 1000000L);
//        logger.debug("query segment length = {}", trajLen);
//        logger.debug("Start to retrieve inverted dataStructure");
//        if (k > 1) return null;
//        //mapMatchedTrajectoryId and score
//        Map<Integer, Double> mmtScore = new HashMap<>();
//        Map<Integer, List<Segment>> mmtSegments = new HashMap<>();
//        for (Segment currentSegment : querySegments) {
//            if (currentSegment == null) continue;
//            Set<Integer> mmtIDs = segmentToMapMatchedTraj.get(currentSegment.getID());
//            if (mmtIDs == null) continue;
//            for (Integer mmtID : mmtIDs) {
//                Double len = mmtScore.get(mmtID);
//                if (len == null) {
//                    mmtScore.put(mmtID, currentSegment.getLength());
//                    List<Segment> newSegments = new LinkedList<>();
//                    mmtSegments.put(mmtID, newSegments);
//                    newSegments.add(currentSegment);
//                } else {
//                    mmtScore.put(mmtID, len + currentSegment.getLength());
//                    mmtSegments.get(mmtID).add(currentSegment);
//                }
//            }
//        }
//        Common.instance.IntBox4.add(mmtSegments.size());
//        //find the top k large values
//        logger.debug("Start to find top k large values");
//        //key for mmtId, value for lcss score
//        beforemm = System.nanoTime();
//        PriorityQueue<Map.Entry<Integer, Double>> topKMMT = new PriorityQueue<>((e1, e2) -> e1.getValue().compareTo(e2.getValue()));
//        List<Map.Entry<Integer, Double>> invertedListResults = new ArrayList<>(mmtScore.entrySet());
//        Collections.sort(invertedListResults, (entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));
//        int count = 0;
//        for (Map.Entry<Integer, Double> invertedListResult : invertedListResults) {
//            ++count;
//            if (topKMMT.size() == k && invertedListResult.getValue() < topKMMT.peek().getValue()) break;
//            double lcssScore = LCSS.run(mmtSegments.get(invertedListResult.getKey()), querySegments, Integer.MAX_VALUE);
//            //update upper bound to exact lcss score
//            invertedListResult.setValue(lcssScore);
//            topKMMT.offer(invertedListResult);
//            if (topKMMT.size() > k) topKMMT.poll();
//        }
//        while (topKMMT.size() > 0) {
//            Map.Entry<Integer, Double> entry = topKMMT.poll();
//            result.add(0, entry.getKey());
//        }
//        Common.instance.IntBox.add(count);
////        Set<Double> scoreSet = new HashSet<>();
////        double minScore = -1;
////        int initSize = k;
////        Iterator<Map.Entry<Integer, Double>> iterator = mmtScore.entrySet().iterator();
////        while (iterator.hasNext()) {
////            Map.Entry<Integer, Double> entry = iterator.next();
////            topKMMT.offer(entry);
////            if (topKMMT.size() > initSize) {
////                if (Double.compare(topKMMT.peek().getValue(), entry.getValue()) == 0) {
////                    ++initSize;
////                } else {
////                    double value = topKMMT.poll().getValue();
////                    while (Double.compare(topKMMT.peek().getValue(), value) == 0) {
////                        topKMMT.poll();
////                        --initSize;
////                    }
////                }
////            }
////        }
////
////        List<MapMatchedTrajectory> tempResult = new LinkedList<>();
////
////        while (topKMMT.size() > 0) {
////            Map.Entry<Integer, Double> entry = topKMMT.poll();
////            MapMatchedTrajectory mapMatchedTrajectory = new MapMatchedTrajectory(mmtSegments.get(entry.getKey()), entry.getKey(), -1);
////            mapMatchedTrajectory.setInvertedScore(entry.getValue());
////            tempResult.add(0, mapMatchedTrajectory);
////        }
////
////        //refine
////        logger.debug("Start to refine the result");
////        beforemm = System.nanoTime();
////        int endPos = k - 1, startPos = k - 1;
////        while (startPos > 0 && tempResult.get(startPos).equals(tempResult.get(startPos - 1)))
////            --startPos;
////        while (endPos < tempResult.size() && tempResult.get(endPos).equals(tempResult.get(endPos - 1)))
////            ++endPos;
////        result = tempResult.subList(0, startPos);
////        tempResult = tempResult.subList(startPos, endPos + 1);
////        Common.instance.IntBox.add(tempResult.size());
////        logger.debug("tempResult size is {}, start to refine the results", tempResult.size());
////        Collections.sort(tempResult, (m1, m2) -> {
////            double lcss1 = LCSS.run(m1.getSegments(), querySegments, Integer.MAX_VALUE);
////            double lcss2 = LCSS.run(m2.getSegments(), querySegments, Integer.MAX_VALUE);
////            return Double.compare(lcss2, lcss1);
////        });
////        result.addAll(tempResult.subList(0, k - result.size()));
//        Common.instance.LongBox2.add((System.nanoTime() - beforemm) / 1000000L);
////        } catch (Exception e) {
////            logger.error("{}, points = {}", e, StringUtils.join(trajectory.getPoints(), ','));
////        }

        return result;
    }


    /**
     * Filter and refine, first retrieve topk elements based on inverted dataStructure, then refine the results
     *
     * @param trajectory
     * @param mapper
     * @param k
     * @return
     */
    public List<Integer> fastRun(Trajectory trajectory, Mapper mapper, int k) {
        logger.info("Enter KNearestNeighbors");
        final List<Segment> querySegments = new ArrayList<>();
        List<Integer> result = new ArrayList<>(k);
//        try {
//        logger.debug("Start to map matching");
//        long beforemm = System.nanoTime();
//        Common.instance.IntBox2.add(trajectory.getPoints().size());
//        List<Segment> rawSegments = trajectoryMapping.match(trajectory.getPoints());
//        Common.instance.IntBox3.add(rawSegments.size());
//        //adjust segments
//        logger.debug("Start to adjust segments");
//        double trajLen = 0;
//        for (Segment rawSegment : rawSegments) {
//            Segment matchedSegment = Common.instance.ALL_SEGMENTS.get(Common.instance.ALL_EDGEMATCHES.get(rawSegment.getKey()));
//            querySegments.add(matchedSegment);
//            trajLen += rawSegment.getLength();
//        }
//        Common.instance.LongBox.add((System.nanoTime() - beforemm) / 1000000L);
//        logger.debug("query segment length = {}", trajLen);
//        logger.debug("Start to retrieve inverted dataStructure");
//
//        //mapMatchedTrajectoryId and score
//        Map<Integer, Double> mmtScore = new HashMap<>();
//        Map<Integer, List<Segment>> mmtSegments = new HashMap<>();
//        for (Segment currentSegment : querySegments) {
//            if (currentSegment == null) continue;
//            Set<Integer> mmtIDs = segmentToMapMatchedTraj.get(currentSegment.getID());
//            if (mmtIDs == null) continue;
//            for (Integer mmtID : mmtIDs) {
//                Double len = mmtScore.get(mmtID);
//                if (len == null) {
//                    mmtScore.put(mmtID, currentSegment.getLength());
//                    List<Segment> newSegments = new LinkedList<>();
//                    mmtSegments.put(mmtID, newSegments);
//                    newSegments.add(currentSegment);
//                } else {
//                    mmtScore.put(mmtID, len + currentSegment.getLength());
//                    mmtSegments.get(mmtID).add(currentSegment);
//                }
//            }
//        }
//        Common.instance.IntBox4.add(mmtSegments.size());
//        //find the top k large values
//        logger.debug("Start to find top k large values");
//        //key for mmtId, value for lcss score
//        beforemm = System.nanoTime();
//        PriorityQueue<Map.Entry<Integer, Double>> topKMMT = new PriorityQueue<>((e1, e2) -> e1.getValue().compareTo(e2.getValue()));
//        List<Map.Entry<Integer, Double>> invertedListResults = new ArrayList<>(mmtScore.entrySet());
//        Collections.sort(invertedListResults, (entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));
//        int count = 0;
//        for (Map.Entry<Integer, Double> invertedListResult : invertedListResults) {
//            ++count;
//            if (topKMMT.size() == k && invertedListResult.getValue() < topKMMT.peek().getValue()) break;
//            double lcssScore = LCSS.run(mmtSegments.get(invertedListResult.getKey()), querySegments, Integer.MAX_VALUE);
//            //update upper bound to exact lcss score
//            invertedListResult.setValue(lcssScore);
//            topKMMT.offer(invertedListResult);
//            if (topKMMT.size() > k) topKMMT.poll();
//        }
//        while (topKMMT.size() > 0) {
//            Map.Entry<Integer, Double> entry = topKMMT.poll();
//            result.add(0, entry.getKey());
//        }
//        Common.instance.IntBox.add(count);
//        Common.instance.LongBox2.add((System.nanoTime() - beforemm) / 1000000L);

        return result;
    }
}
