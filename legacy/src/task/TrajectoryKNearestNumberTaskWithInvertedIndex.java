package au.edu.rmit.trajectory.similarity.task;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.LongestCommonSubsequence;
import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.datastructure.EdgeInvertedIndexNoPos;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author forrest0402
 * @Description
 * @date 11/16/2017
 */
@Component
@Scope("prototype")
public class TrajectoryKNearestNumberTaskWithInvertedIndex implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(TrajectoryKNearestNumberTaskWithInvertedIndex.class);

    @Autowired
    TrajectoryService trajectoryService;

    @Autowired
    LongestCommonSubsequence longestCommonSubsequence;

    @Autowired
    Mapper mapper;

    final EdgeInvertedIndexNoPos index;

    final Trajectory queryTrajectory;

    final List<String> queryAns;

    final MeasureType measureType;

    final int k;

    final AtomicInteger candidateNumber;
    final AtomicInteger scanCandidateNumber;
    final AtomicInteger noCandidateNumber;

    final GPXEntry g = new GPXEntry(Common.instance.g.getLat(), Common.instance.g.getLon(), 0);

    public TrajectoryKNearestNumberTaskWithInvertedIndex(EdgeInvertedIndexNoPos index, Trajectory trajectory, List<String> queryAns, MeasureType measureType, int k, AtomicInteger candidateNumber, AtomicInteger scanCandidateNumber, AtomicInteger noCandidateNumber) {
        this.index = index;
        this.queryTrajectory = trajectory;
        this.queryAns = queryAns;
        this.measureType = measureType;
        this.k = k;
        this.candidateNumber = candidateNumber;
        this.scanCandidateNumber = scanCandidateNumber;
        this.noCandidateNumber = noCandidateNumber;
    }

    @Override
    public void run() {
        List<MMEdge> queryMMEdges = new ArrayList<>();
        mapper.fastMatch(queryTrajectory.getPoints(), new ArrayList<>(), queryMMEdges);
        //key is trajectory hash, value is upperbound
        Map<Integer, Double> ids = index.query(queryMMEdges);
        if (ids.size() < k) {
            logger.error("no candidate: {}", queryTrajectory.getId());
            noCandidateNumber.incrementAndGet();
            return;
        }
        candidateNumber.addAndGet(ids.size());
        PriorityQueue<Map.Entry<Integer, Double>> candidateHeap = new PriorityQueue<>((c1, c2) -> c2.getValue().compareTo(c1.getValue()));
        ids.entrySet().forEach(entry -> candidateHeap.add(entry));
        ids = null;
        PriorityQueue<Map.Entry<Integer, Double>> resHeap = new PriorityQueue<>(Map.Entry.comparingByValue());
        double bestSoFar = Double.MIN_VALUE;
        //key for trajectory hash, value for score
        Map.Entry<Integer, Double> topElem = candidateHeap.poll();
        while ((topElem != null && bestSoFar < topElem.getValue()) || resHeap.size() < k) {
            scanCandidateNumber.incrementAndGet();
            List<MMEdge> candidate = index.getMMTrajectory(topElem.getKey());
            if (topElem.getKey() == 372559)
                System.out.print("");
            double score = 0;
            switch (this.measureType) {
                case LORS:
                    score = longestCommonSubsequence.mmRun(queryMMEdges, candidate, Integer.MAX_VALUE);
                    break;
                default:
                    throw new IllegalArgumentException("measure type is illegal");
            }
            topElem.setValue(score);
            resHeap.add(topElem);
            if (resHeap.size() > k) resHeap.poll();
            topElem = candidateHeap.poll();
            bestSoFar = resHeap.peek().getValue();
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(queryTrajectory.getId())
                .append(" ");
        while (resHeap.size() > 0) {
            Map.Entry<Integer, Double> elem = resHeap.poll();
            stringBuilder.append(elem.getKey()).append(",").append(elem.getValue()).append(" ");
        }
        synchronized (TrajectoryKNearestNumberTaskWithInvertedIndex.class) {
            queryAns.add(stringBuilder.toString());
            System.out.println(queryAns.size());
        }
    }
}
