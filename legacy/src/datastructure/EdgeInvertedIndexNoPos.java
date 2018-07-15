package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.algorithm.Mapper;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * inverted dataStructure where key is EdgeId, value is a list of trajectories.
 *
 * @author forrest0402
 * @Description
 * @date 12/20/2017
 */
@Component
public class EdgeInvertedIndexNoPos {

    private static Logger logger = LoggerFactory.getLogger(EdgeInvertedIndexNoPos.class);

    @Autowired
    TrajectoryService trajectoryService;

    @Autowired
    Mapper mapper;

    @Autowired
    public Environment environment;

    List<Trajectory> trajectoryList = null;

    /**
     * key: edgeId
     */
    private Map<Integer, Set<Integer>> index = null;

    private Map<Integer, List<MMEdge>> allTrajectories = null;

    public void buildIndex(List<Trajectory> trajectoryList) {
        logger.info("start to buildTorGraph dataStructure");
        this.trajectoryList = trajectoryList;
        this.allTrajectories = new HashMap<>();
        this.index = new ConcurrentHashMap<>();
        mapper.readPBF(environment.getProperty("PORTO_PBF_FILE_PATH"), "./target/mapmatchingtest");
        AtomicInteger counter = new AtomicInteger(0);
        ExecutorService threadPool = new ThreadPoolExecutor(10, 10, 60000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        for (Trajectory trajectory : trajectoryList) {
            threadPool.execute(new Task(trajectory, counter));
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("Exit building dataStructure, trajectory size is: {}", allTrajectories.size());
    }

    public List<MMEdge> getMMTrajectory(int id) {
        return allTrajectories.get(id);
    }

    public Map<Integer, Double> query(List<MMEdge> edges) {
        Map<Integer, Double> res = new HashMap<>();
        for (MMEdge edge : edges) {
            Set<Integer> trajIds = index.get(edge.hashCode());
            for (Integer trajId : trajIds) {
                Double score = res.get(trajId);
                if (score == null) {
                    res.put(trajId, edge.getLength());
                } else {
                    res.put(trajId, score + edge.getLength());
                }
            }
        }
        return res;
    }

    class Task implements Runnable {

        final Trajectory trajectory;

        final AtomicInteger process;

        public Task(Trajectory trajectory, AtomicInteger process) {

            this.trajectory = trajectory;
            this.process = process;
        }

        @Override
        public void run() {
            process.incrementAndGet();
            if (process.intValue() % 1000 == 0)
                System.out.println("buildTorGraph dataStructure: " + process.intValue());
            try {
                List<MMEdge> edges = new ArrayList<>();
                mapper.fastMatchMMPoint(trajectory.getMMPoints(), new ArrayList<>(), edges);
                synchronized (EdgeInvertedIndexNoPos.class) {
                    allTrajectories.put(trajectory.getId(), edges);
                }
                for (MMEdge edge : edges) {
                    synchronized (EdgeInvertedIndexNoPos.class) {
                        Set<Integer> trajIds = index.computeIfAbsent(edge.hashCode(), (k) -> new HashSet<>());
                        trajIds.add(trajectory.getId());
                    }
                }
            } catch (Exception e) {
                //logger.error("{}", e);//some trajectories cannot be matched
            }
        }
    }
}
