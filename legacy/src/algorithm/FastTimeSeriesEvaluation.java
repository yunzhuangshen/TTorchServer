package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.datastructure.ComplexTraGridIndex;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.model.GridIndexPoint;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Short for FTSE, see 'An Efficient and Accurate Method for Evaluating Time Series Similarity' for details
 *
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
@Component
public class FastTimeSeriesEvaluation {

    private static Logger logger = LoggerFactory.getLogger(FastTimeSeriesEvaluation.class);

    /**
     * key for trajectory hash, value for its length
     */
    private Map<Integer, Integer> trajectorySizeMap;

    /**
     * key for trajecoty_point_position, value for that point
     */

    //for porto final float minLat = 41.108936f, maxLat = 41.222659f, minLon = -8.704896f, maxLon = -8.489324f;

    //for beijing
    final float minLat = 39.587306f, maxLat = 40.334641f, minLon = 115.825296f, maxLon = 116.789346f;

    @Autowired
    private ComplexTraGridIndex complexTraGridIndex;

    public void load() {

    }

    /**
     * buildTorGraph grid dataStructure for every point of all trajectories
     *
     * @param trajectoryList
     * @param epsilon        the length of each grid
     */
    public void init(Collection<Trajectory> trajectoryList, double epsilon) {
        logger.info("Enter build");
        trajectorySizeMap = new HashMap<>();
        for (Trajectory trajectory : trajectoryList) {
            trajectorySizeMap.put(trajectory.getId(), trajectory.getPoints().size());
        }
        if (!complexTraGridIndex.load()) {
            logger.info("grid dataStructure does not exist, start to buildTorGraph, trajectory size: {}", trajectoryList.size());
            List<GridIndexPoint> pointList = new LinkedList<>();
            Iterator<Trajectory> iter = trajectoryList.iterator();
            ExecutorService threadPool = new ThreadPoolExecutor(15, 18, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            AtomicInteger validTrajectory = new AtomicInteger(0);
            while (iter.hasNext()) {
                Trajectory trajectory = iter.next();
                threadPool.execute(() -> {
                    Short pos = 1;
                    if (trajectory.getId() % 10000 == 0)
                        logger.info("trajectory ID: {}", trajectory.getId());
                    List<GridIndexPoint> list = new LinkedList<>();
                    boolean valid = true;
                    for (MMPoint point : trajectory.getMMPoints()) {
                        list.add(new GridIndexPoint((float) point.getLat(), (float) point.getLon(), trajectory.getId(), pos++));
//                        if (point.getLat() < minLat || point.getLat() > maxLat || point.getLon() < minLon || point.getLon() > maxLon) {
//                            valid = false;
//                            break;
//                        }
                    }
                    if (valid) {
                        validTrajectory.incrementAndGet();
                        synchronized (FastTimeSeriesEvaluation.class) {
                            pointList.addAll(list);
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
            for (Trajectory trajectory : trajectoryList) {
                trajectory.clear();
            }
            trajectoryList.clear();
            logger.info("valid trajectory size: {}, point size: {}", validTrajectory.intValue(), pointList.size());
            complexTraGridIndex.buildIndex(pointList, (float) epsilon);
        }
        logger.info("Exit build - trajectorySizeMap size: {}", trajectorySizeMap.size());
    }

    /**
     * score = max
     *
     * @param matches
     * @param L
     * @param max
     * @return
     */
    private int longestCommonSubsequenceComputatation(int[] matches, List<Short> L, int max) {
        int temp = matches[0], c = 0;
        for (short k : L) {
            if (temp < k) {
                while (matches[c] < k) ++c;
                temp = matches[c];
                matches[c] = k;
                if (c > max)
                    max = c;
            }
        }
        return max;
    }

    /**
     * score = max - (m + n)
     *
     * @param matches
     * @param L
     * @param max
     * @param m
     * @return
     */
    private int editDistanceComputation(int[] matches, List<Short> L, int max, int m) {
        int temp = matches[0], temp2 = matches[0], c = 0;
        for (short k : L) {
            if (temp < k) {
                while (matches[c] < k) {
                    if (temp < matches[c] - 1 && temp < m - 1) {
                        temp2 = matches[c];
                        matches[c] = temp + 1;
                        temp = temp2;
                    } else temp = matches[c];
                    c++;
                }
                temp2 = matches[c];
                matches[c] = temp + 1;
                temp = matches[c + 1];
                if (matches[c + 1] > k) matches[c + 1] = k;
                if (max < c + 1) max = c + 1;
                c += 2;
            } else if (temp2 < k && k < matches[c]) {
                temp2 = temp;
                temp = matches[c];
                matches[c] = k;
                if (max < c) max = c;
                ++c;
            }
        }
        for (int j = c; j <= max + 1; ++j) {
            if (temp < matches[j] - 1 && temp < m - 1) {
                temp2 = matches[j];
                matches[j] = temp + 1;
                temp = temp2;
                if (max < j) max = j;
            } else temp = matches[j];
        }
        return max;
    }

    /**
     * @param trajectory
     * @param k
     * @param measureType only support EDR and LCSS
     * @return list of trajectory ids
     */
    public List<Integer> findTopK(Map<Integer, Trajectory> trajectoryMap, Trajectory trajectory, int k, MeasureType measureType, List<Integer> candidateNumberList) {
        List<MMPoint> queryPoints = trajectory.getMMPoints();
        //matches array for candidate trajectory
        Map<Integer, int[]> matchMap = new HashMap<>();
        //key for trajectory hash, value for its max value
        Map<Integer, Integer> maxValueMap = new HashMap<>();
        //for each query point
        for (MMPoint queryPoint : queryPoints) {
            //key for trajectory hash, value for point position list
            Map<Integer, List<Short>> candidateMap = complexTraGridIndex.find(queryPoint);
            if (candidateMap == null) {
                continue;
            }
            //refine the candidate points because not all points are valid
            Iterator<Map.Entry<Integer, List<Short>>> iterator = candidateMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, List<Short>> next = iterator.next();
                Iterator<Short> positionIter = next.getValue().iterator();
                Trajectory curTrajectory = trajectoryMap.get(next.getKey());
                while (positionIter.hasNext()) {
                    short position = positionIter.next();
                    double distance = GeoUtil.distance(queryPoint, curTrajectory.getMMPoints().get(position - 1)); //position starts from 1
                    if (distance > 100.0) positionIter.remove();
                }
                if (next.getValue().size() == 0)
                    iterator.remove();
            }
            if (candidateMap == null) {
                logger.error("the query is out of grid dataStructure. Trajectory: ({},{},{})", trajectory.getId(), queryPoint.getLat(), queryPoint.getLon());
                continue;
            }
            //for each trajectory list which intersects with the point
            for (Map.Entry<Integer, List<Short>> listEntry : candidateMap.entrySet()) {
                int trajectoryID = listEntry.getKey();
                int[] matches = matchMap.get(trajectoryID);
                //when building dataStructure, I use all trajectories, but some trajectories are filtered out after map matching
                if (!this.trajectorySizeMap.containsKey(trajectoryID)) {
                    continue;
                }
                int m = this.trajectorySizeMap.get(trajectoryID);
                if (matches == null) {
                    matches = new int[(queryPoints.size() + 1) * 2];
                    //initialize matches
                    matches[0] = 0;
                    int max = 0;
                    for (int i = 1; i < matches.length; ++i)
                        matches[i] = m + 1;
                    switch (measureType) {
                        case LCSS:
                            max = longestCommonSubsequenceComputatation(matches, listEntry.getValue(), 0);
                            break;
                        case EDR:
                            max = editDistanceComputation(matches, listEntry.getValue(), 0, m);
                            break;
                        default:
                            throw new IllegalStateException("only support EDR and LCSS");
                    }
                    maxValueMap.put(listEntry.getKey(), max);
                    matchMap.put(listEntry.getKey(), matches);
                } else {
                    int max = maxValueMap.get(listEntry.getKey());
                    switch (measureType) {
                        case LCSS:
                            max = longestCommonSubsequenceComputatation(matches, listEntry.getValue(), max);
                            break;
                        case EDR:
                            max = editDistanceComputation(matches, listEntry.getValue(), max, m);
                            break;
                        default:
                            throw new IllegalStateException("only support EDR and LCSS");
                    }
                    maxValueMap.put(listEntry.getKey(), max);
                }
            }
        }
        if (candidateNumberList != null) {
            candidateNumberList.add(maxValueMap.size());
            logger.info("candidate number: {}", maxValueMap.size());
        }
        //find final top k result
        PriorityQueue<Map.Entry<Integer, Integer>> result = new PriorityQueue<>(Map.Entry.comparingByValue());
        for (Map.Entry<Integer, Integer> entry : maxValueMap.entrySet()) {
            if (measureType == MeasureType.EDR) {
                int score = entry.getValue() - (this.trajectorySizeMap.get(entry.getKey()) + queryPoints.size());
                entry.setValue(score);
            }
            result.add(entry);
            if (result.size() > k)
                result.poll();
        }

        return result.stream().map(c -> c.getKey()).collect(Collectors.toList());
    }
}
