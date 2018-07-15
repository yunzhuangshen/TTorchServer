package au.edu.rmit.trajectory.similarity.task;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.datastructure.RTreeWrapper;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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
public class TrajectoryKNearestNumberTask implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(TrajectoryKNearestNumberTask.class);

    @Autowired
    TrajectoryService trajectoryService;

    final RTreeWrapper envelopeIndex;

    final Trajectory trajectory;

    final List<String> queryAns;

    final SimilarityMeasure similarityMeasure;

    final MeasureType measureType;

    final int k;

    final AtomicInteger candidateNumber;
    final AtomicInteger scanCandidateNumber;
    final AtomicInteger noCandidateNumber;

    final GPXEntry g = new GPXEntry(Common.instance.g.getLat(), Common.instance.g.getLon(), 0);

    public TrajectoryKNearestNumberTask(RTreeWrapper envelopeIndex, Trajectory trajectory, List<String> queryAns, SimilarityMeasure<?> similarityMeasure, MeasureType measureType, int k, AtomicInteger candidateNumber, AtomicInteger scanCandidateNumber, AtomicInteger noCandidateNumber) {
        this.envelopeIndex = envelopeIndex;
        this.trajectory = trajectory;
        this.queryAns = queryAns;
        this.similarityMeasure = similarityMeasure;
        this.measureType = measureType;
        this.k = k;
        this.candidateNumber = candidateNumber;
        this.scanCandidateNumber = scanCandidateNumber;
        this.noCandidateNumber = noCandidateNumber;
    }

    @Override
    public void run() {
        Map<Integer, Double> ids = envelopeIndex.query(trajectory);
        if (ids.size() < k) {
            logger.error("no candidate: {}", trajectory.getId());
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
            Trajectory candidate = trajectoryService.getTrajectory(topElem.getKey());
            double score = 0;
            switch (this.measureType) {
                case LCSS:
                    score = similarityMeasure.LongestCommonSubsequence(trajectory.getPoints(), candidate.getPoints(), Integer.MAX_VALUE);
                    break;
                case EDR:
                    score = -similarityMeasure.EditDistanceonRealSequence(trajectory.getPoints(), candidate.getPoints());
                    break;
                case DTW:
                    score = -similarityMeasure.DynamicTimeWarping(trajectory.getPoints(), candidate.getPoints());
                    break;
                case ERP:
                    score = -similarityMeasure.EditDistanceWithRealPenalty(trajectory.getPoints(), candidate.getPoints(), g);
                    break;
            }
            topElem.setValue(score);
            resHeap.add(topElem);
            if (resHeap.size() > k) resHeap.poll();
            topElem = candidateHeap.poll();
            bestSoFar = resHeap.peek().getValue();
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(trajectory.getId())
                .append(" ");
        while (resHeap.size() > 0) {
            Map.Entry<Integer, Double> elem = resHeap.poll();
            stringBuilder.append(elem.getKey()).append(",").append(elem.getValue()).append(" ");
        }
        synchronized (TrajectoryKNearestNumberTask.class) {
            queryAns.add(stringBuilder.toString());
            System.out.println(queryAns.size());
        }
    }
}
