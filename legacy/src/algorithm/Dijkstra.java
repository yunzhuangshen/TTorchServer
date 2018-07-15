package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.datastructure.FibonacciHeap;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.torch.dataStructure.ShortestPathCache;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import au.edu.rmit.trajectory.torch.dataStructure.TorGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * calculate the shortest path from a start candidatePoint s to its adjacent points (tower points) limited by a max distance
 * the result will be store in ShortestPathCache
 *
 * @author forrest0402
 * @Description
 * @date 12/4/2017
 */
public class Dijkstra {

    private static Logger logger = LoggerFactory.getLogger(Dijkstra.class);

    // search radius around src point
    // metric: meter
    private final double SEARCH_RANGE = 1000;

    final Map<Integer, MMPoint> allPoints;

    final Map<String, MMEdge> allEdges;

    public Dijkstra(TorGraph graph) {
        this.allPoints = graph.allPoints;
        this.allEdges = graph.allEdges;
    }

    /**
     * from source point, calculate min path to other tower points that are within maxDistance around src
     * relevant information are recorded and modeled into ShortestPathCache.ShortestPathEntry
     *
     * @param src the tower point as the source
     *
     * @see ShortestPathCache.ShortestPathEntry
     */
    public void run(MMPoint src, ShortestPathCache pool) {
        Map<Integer, Double> dist = new HashMap<>();
        Set<Integer> visited = new HashSet<>();
        Map<Integer, List<MMPoint>> routing = new HashMap<>();
        Map<Integer, FibonacciHeap.Entry<MMPoint>> fibEntries = new HashMap<>();
        FibonacciHeap<MMPoint> priorityQ = new FibonacciHeap<>();

        Iterator<MMPoint> itr = src.adjIterator();
        while (itr.hasNext()) {
            MMPoint adjPoint = itr.next();
            FibonacciHeap.Entry<MMPoint> entry = priorityQ.enqueue(adjPoint, GeoUtil.distance(src, adjPoint));
            fibEntries.put(adjPoint.hashCode(), entry);
            List<MMPoint> route = new LinkedList<>();
            route.add(adjPoint);
            routing.put(adjPoint.hashCode(), route);
        }

        dist.put(src.hashCode(), 0.0);
        List<MMPoint> toSelf = Collections.singletonList(src);
        routing.put(src.hashCode(), toSelf);

        while (priorityQ.size() > 0) {
            FibonacciHeap.Entry<MMPoint> entry = priorityQ.dequeueMin();
            if (visited.contains(entry.getValue().hashCode()))
                continue;
            visited.add(entry.getValue().hashCode());
            dist.put(entry.getValue().hashCode(), entry.getPriority());
            if (entry.getPriority() >= SEARCH_RANGE) break;

            itr = entry.getValue().adjIterator();
            while (itr.hasNext()) {
                MMPoint adjPoint = itr.next();
                Double oldValue = dist.get(adjPoint.hashCode());
                double newValue = entry.getPriority() + entry.getValue().getAdjDistance(adjPoint);
                if (oldValue != null) {
                    if (oldValue > newValue) {
                        priorityQ.decreaseKey(fibEntries.get(adjPoint.hashCode()), newValue);
                        dist.put(adjPoint.hashCode(), newValue);
                        List<MMPoint> route = new ArrayList<>(routing.get(entry.getValue().hashCode()));
                        route.add(adjPoint);
                        routing.put(adjPoint.hashCode(), route);
                    }
                } else {
                    dist.put(adjPoint.hashCode(), newValue);
                    fibEntries.put(adjPoint.hashCode(), priorityQ.enqueue(adjPoint, newValue));
                    List<MMPoint> route = new ArrayList<>(routing.get(entry.getValue().hashCode()));
                    route.add(adjPoint);
                    routing.put(adjPoint.hashCode(), route);
                }
            }
        }
        pool.addEntry(src, dist, routing);
    }

    /**
     * 1. for every pillow point, compute the distance to its base point on edge.
     * 2. for every edge, compute the length of it.( distance of two tower points)
     */
    public void post() {
        //initialize toBasePointDistance of Point
        for (MMPoint mmPoint : allPoints.values()) {
            if (!mmPoint.isTowerPoint) {
                MMPoint pre = mmPoint.edge.basePoint;
                double len = 0;
                for (MMPoint point : mmPoint.edge.getPillarPoints()) {
                    len += GeoUtil.distance(pre, point);
                    if (point == mmPoint) break;
                    pre = point;
                }
                mmPoint.toBasePointDistance = len;
            }
        }
        //initialize getLength of Edge
        for (MMEdge mmEdge : allEdges.values()) {
            mmEdge.getLength();
        }
    }
}