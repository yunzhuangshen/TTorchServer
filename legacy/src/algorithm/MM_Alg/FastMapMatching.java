package au.edu.rmit.trajectory.similarity.algorithm.MM_Alg;

import au.edu.rmit.trajectory.similarity.algorithm.Dijkstra;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.torch.dataStructure.ShortestPathCache;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.graphhopper.GraphHopper;
import com.graphhopper.routing.util.AllEdgesIterator;
import com.graphhopper.storage.Graph;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.DistancePlaneProjection;
import com.graphhopper.util.PointList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FastMapMatching {

    Logger logger = LoggerFactory.getLogger(FastMapMatching.class);

    private final GraphHopper hopper;
    private final Graph graph;
    private DistanceCalc distanceCalc = new DistancePlaneProjection();

    //used for fast map matching
    final double MAX_DISTANCE = 1000;
    final double SEARCH_RANGE = 50;
    private ShortestPathCache shortestPathCache = null;

    private Map<Integer, MMPoint> towerPoints = null;
    private Map<Integer, MMPoint> allPoints = null;

    //key for TorSegment.getKey(towerPoint1, towerPoint2), for edges between the same tower points, only the shortest one will be stored
    private Map<String, MMEdge> allEdges = null;
    private Set<MMEdge> allEdgeSet;
    private RTree<MMPoint, Geometry> rTree = null;

    public FastMapMatching(GraphHopper hopper){

        this.hopper = hopper;
        graph = hopper.getGraphHopperStorage();
        readOSM2Ram();

        shortestPathbetweenTowerPoints();
    }

    /**
     * read .OSM.PBF file to memory.
     * data is stored in lists and maps.
     * buildTorGraph virtual graph( using tower points only)
     */
    private void readOSM2Ram() {
        logger.info("Enter - readOSM2Ram");
        logger.info("start to read graph into memory");

        this.towerPoints = new HashMap<>();
        this.allPoints = new HashMap<>();
        this.allEdges = new HashMap<>();
        this.allEdgeSet = new HashSet<>();
        this.rTree = RTree.star().maxChildren(6).create();

        for (int i = 0; i < this.graph.getNodes(); ++i) {
            MMPoint towerPoint = new MMPoint(this.graph.getNodeAccess().getLat(i), this.graph.getNodeAccess().getLon(i), true);
            towerPoints.put(towerPoint.hashCode(), towerPoint);
            allPoints.put(towerPoint.hashCode(), towerPoint);
        }


        AllEdgesIterator allEdgeIterator = this.graph.getAllEdges();
        //the number of paths between two tower points can be larger than 1
        while (allEdgeIterator.next()) {

            //fetch tower points as well as pillow points
            PointList pointList = allEdgeIterator.fetchWayGeometry(3);
            MMPoint startTowerPoint = towerPoints.get(new MMPoint(pointList.getLat(0), pointList.getLon(0)).hashCode());
            MMEdge edge = new MMEdge();
            for (int i = 1; i < pointList.size(); ++i) {
                MMPoint point = new MMPoint(pointList.getLat(i), pointList.getLon(i));
                if (towerPoints.containsKey(point.hashCode())) {
                    point = towerPoints.get(point.hashCode());

                    edge.basePoint = startTowerPoint;
                    edge.adjPoint = point;
                    this.allEdgeSet.add(edge);

                    if (allEdgeIterator.isForward(hopper.getEncodingManager().fetchEdgeEncoders().get(0))) {
                        startTowerPoint.addAdjPoint(point);
                        MMEdge oldEdge = allEdges.get(MMEdge.getKey(startTowerPoint, point));
                        if (oldEdge == null || oldEdge.getLength() > edge.getLength())
                            allEdges.put(MMEdge.getKey(startTowerPoint, point), edge);
                        edge.isForward = true;
                    }

                    if (allEdgeIterator.isBackward(hopper.getEncodingManager().fetchEdgeEncoders().get(0))) {
                        point.addAdjPoint(startTowerPoint);
                        MMEdge oldEdge = allEdges.get(MMEdge.getKey(point, startTowerPoint));
                        if (oldEdge == null || oldEdge.getLength() > edge.getLength())
                            allEdges.put(MMEdge.getKey(point, startTowerPoint), edge);
                        edge.isBackward = true;
                    }

                    edge = new MMEdge();
                    startTowerPoint = point;
                } else {
                    edge.addPillarPoint(point);
                    point.edge = edge;
                    allPoints.put(point.hashCode(), point);
                }
            }
        }
    }

    /**
     * for each tower point, compute and record the distance of it with other tower points within a distance threshold.
     * compute and record distance of each pillow point and its base point.
     * compute and record length of each edge.
     * add density to edges.
     * dataStructure to rtree
     */
    private ShortestPathCache shortestPathbetweenTowerPoints() {
        logger.info("Enter - shortestPathbetweenTowerPoints");
        logger.info("precompute shortest path using Dijkstra (SPFA)");

        //for each tower point, compute the shortest path to other tower points that are within a distance threshold
        Dijkstra dijkstra = new Dijkstra(allPoints, allEdges);
        double totalCount = towerPoints.size(), count = 0;
        for (MMPoint mmPoint : allPoints.values()) {
            if (mmPoint.isTowerPoint) {
                dijkstra.run(mmPoint, MAX_DISTANCE);
                if (count++ % 30000 == 0 || count == totalCount) {
                    System.out.println(count * 100 / totalCount);
                }
            }
        }
        logger.info("dijkstra post - 1) adding points, 2) indexing points, 3) calculating length of edges");

        for (MMEdge edge : allEdges.values()) {

            if (edge.getPillarPoints().size() == 0 && GeoUtil.distance(edge.basePoint, edge.adjPoint) >= SEARCH_RANGE * 2) {
                MMPoint pillarPoint = new MMPoint(edge.basePoint, edge.adjPoint, edge);
                edge.getPillarPoints().add(pillarPoint);
                allPoints.put(pillarPoint.hashCode(), pillarPoint);
            }
            if (edge.isForward) {
                MMPoint pre = edge.basePoint;
                List<MMPoint> pillarPoints = edge.getPillarPoints();
                pillarPoints.add(edge.adjPoint);
                for (int i = 0; i < pillarPoints.size(); ++i) {
                    double dis = GeoUtil.distance(pre, pillarPoints.get(i));
                    if (dis >= SEARCH_RANGE * 1.5) {
                        MMPoint pillarPoint = new MMPoint(pre, pillarPoints.get(i), edge);
                        pillarPoints.add(i, pillarPoint);
                        allPoints.put(pillarPoint.hashCode(), pillarPoint);
                        --i;
                    } else pre = pillarPoints.get(i);
                }
                pillarPoints.remove(edge.adjPoint);
            }
            if (edge.isBackward) {
                MMPoint pre = edge.adjPoint;
                List<MMPoint> pillarPoints = edge.getPillarPoints();
                pillarPoints.add(0, edge.basePoint);
                for (int i = pillarPoints.size() - 1; i >= 0; --i) {
                    double dis = GeoUtil.distance(pre, pillarPoints.get(i));
                    if (dis >= SEARCH_RANGE * 1.5) {
                        MMPoint pillarPoint = new MMPoint(pre, pillarPoints.get(i), edge);
                        pillarPoints.add(i + 1, pillarPoint);
                        allPoints.put(pillarPoint.hashCode(), pillarPoint);
                        i += 2;
                    } else
                        pre = pillarPoints.get(i);
                }
                pillarPoints.remove(edge.basePoint);
            }
        }

        for (MMPoint point : allPoints.values()) {
            this.rTree = this.rTree.add(point, Geometries.pointGeographic(point.getLon(), point.getLat()));
        }

        dijkstra.post();

        logger.info("Exit - shortestPathbetweenTowerPoints. There are {} edges, {} points including {} tower points.", allEdges.size(), allPoints.size(), towerPoints.size());
    }



    /**
     * map trajectory nodes( arg1) to nodes( arg2) and edges( arg3) on map
     *
     * @param pointList a list of points to be mapped
     * @param pathPoints mapped points
     * @param pathEdges mapped edges
     * @return
     */
    public double runMMPoint(List<MMPoint> pointList, List<MMPoint> pathPoints, List<MMEdge> pathEdges) {

        pointList = filterMMPoints(pointList);

        // at least, a trajectory should be defined with 2 points.
        if (pointList.size() < 2) {
            logger.error("Too few coordinates in input file ("
                    + pointList.size() + "). Correct format?");
            throw new IllegalStateException("the road is broken");
        }

        // find candidates for each candidate point
        List<List<FastMapMatching.Candidate>> candidateSet = new ArrayList<>();
        for (MMPoint point : pointList) {

            final List<FastMapMatching.Candidate> candidates = new LinkedList<>();
            double delta = 0; // increment in meter

            //if rtree cannot retrieve any point within the search window, then expand the range until find candidate point
            while (candidates.size() == 0) {
                rx.Observable<Entry<MMPoint, Geometry>> results = rTree.search(Geometries.rectangleGeographic(
                        GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -(SEARCH_RANGE + delta)),
                        GeoUtil.increaseLatitude(point.getLat(), -(SEARCH_RANGE + delta)),
                        GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), (SEARCH_RANGE + delta)),
                        GeoUtil.increaseLatitude(point.getLat(), (SEARCH_RANGE + delta)))
                );

                results.forEach(entry -> {
                    FastMapMatching.Candidate candidate = new FastMapMatching.Candidate(point, entry.value());
                    candidates.add(candidate);
                });

                delta += 10;
            }

            //normalize
            double sumEmissionProb = 0;
            for (FastMapMatching.Candidate candidate : candidates) {
                sumEmissionProb += candidate.emissionProbability;
            }
            for (FastMapMatching.Candidate candidate : candidates) {
                candidate.emissionProbability /= sumEmissionProb;
            }
            candidateSet.add(candidates);
        }

        //vertibi algorithm
        List<FastMapMatching.Candidate> preCandidates = candidateSet.get(0);
        for (FastMapMatching.Candidate preCandidate : preCandidates) {
            preCandidate.probability = 0;
            for (FastMapMatching.Candidate candidate : candidateSet.get(1)) {
                double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, candidate.candidatePoint);
                double shortestPathDistance = shortestPathCache.minDistance(preCandidate.candidatePoint, candidate.candidatePoint);
                double transitionProbability = lineDistance / shortestPathDistance;
                if (shortestPathDistance == 0) transitionProbability = 1.0;
                preCandidate.probability += preCandidate.emissionProbability * transitionProbability;
            }
        }

        normalization(preCandidates);

        for (int i = 1; i < candidateSet.size(); ++i) {
            List<FastMapMatching.Candidate> curCandidates = candidateSet.get(i);
            double maxProb = Double.MIN_VALUE;
            for (FastMapMatching.Candidate preCandidate : preCandidates) {
                for (FastMapMatching.Candidate curCandidate : curCandidates) {
                    double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, curCandidate.candidatePoint);
                    double shortestPathDistance = shortestPathCache.minDistance(preCandidate.candidatePoint, curCandidate.candidatePoint);
                    double transitionProbability = lineDistance / shortestPathDistance;
                    if (shortestPathDistance == 0) transitionProbability = 1.0;
                    double p = preCandidate.probability * transitionProbability * curCandidate.emissionProbability;
                    if (p > curCandidate.probability) {
                        curCandidate.probability = p;
                        curCandidate.preCandidate = preCandidate;
                    }
                    if (p > maxProb)
                        maxProb = p;
                }
            }
            preCandidates = curCandidates;
            if (maxProb < 1e-150)
                normalization(preCandidates);
        }

        //get the result, first find the candidate from the last candidate set with the maximum probability,
        //then find the optimal candidate chain
        //finally get the shortest path between two candidates
        FastMapMatching.Candidate maxCandidate = null;
        double maxProbability = Double.MIN_VALUE;
        for (FastMapMatching.Candidate candidate : candidateSet.get(candidateSet.size() - 1)) {
            if (candidate.probability > maxProbability) {
                maxProbability = candidate.probability;
                maxCandidate = candidate;
            }
        }

        if (maxCandidate == null) {
            //logger.error("the road is broken {}", pointList);
            throw new IllegalStateException("the road is broken");
        }

        List<MMPoint> candidatePoints = new LinkedList<>();
        List<MMPoint> resPoints = new ArrayList<>();

        while (maxCandidate != null) {
            candidatePoints.add(0, maxCandidate.candidatePoint);
            maxCandidate = maxCandidate.preCandidate;
        }

        MMPoint prePoint = candidatePoints.get(0);
        resPoints.add(prePoint);
        double pathLength = 0;

        for (int i = 1; i < candidatePoints.size(); ++i) {
            if (prePoint == candidatePoints.get(i)) continue;
            double len = shortestPathCache.minDistance(prePoint, candidatePoints.get(i));
            List<MMPoint> shortestPath = shortestPathCache.shortestPath(prePoint, candidatePoints.get(i));
            //logger.info("shortest path of ({},{}) to ({},{}).", prePoint.getLat(), prePoint.getLon(), candidatePoints.get(i).getLat(), candidatePoints.get(i).getLon());
            //printPoints(shortestPath);
            if (shortestPath != null && shortestPath.size() > 0)
                resPoints.addAll(shortestPath);
            else //cannot find shortest path within 1000 m, the point is broken{
            {
                resPoints.add(candidatePoints.get(i));
                //logger.error("the road is broken {}", pointList);
                throw new IllegalStateException("the road is broken");
            }
            pathLength += len;
            prePoint = candidatePoints.get(i);
        }

        if (pathEdges != null) {
            MMPoint pre = resPoints.get(0);
            for (int i = 1; i < resPoints.size(); ++i) {
                MMEdge curEdge = allEdges.get(MMEdge.getKey(pre, resPoints.get(i)));
                if (curEdge == null)
                    curEdge = allEdges.get(MMEdge.getKey(resPoints.get(i), pre));
                if (curEdge != null)
                    pathEdges.add(curEdge);
                else if (!pre.isTowerPoint)
                    pathEdges.add(pre.edge);
                pre = resPoints.get(i);
            }
        }

        //filter out pillar points excluding the start point and end point
        if (pathPoints != null) {
            MMPoint pre = null;
            for (int i = 0; i < resPoints.size(); ++i) {
                if (i != 0 && i != resPoints.size() - 1 && !resPoints.get(i).isTowerPoint) continue;
                if (resPoints.get(i) != pre) {
                    pre = resPoints.get(i);
                    pathPoints.add(pre);
                }
            }
        }
        return pathLength;
    }

    /**
     * reduce density of an query trajectory.
     *
     * @param pointList
     * @return
     */
    private List<MMPoint> filterMMPoints(List<MMPoint> pointList) {
        List<MMPoint> ret = new ArrayList<>();
        MMPoint prevEntry = null;
        int last = pointList.size() - 1;
        for (int i = 0; i <= last; i++) {
            MMPoint point = pointList.get(i);
            if (i == 0 || i == last

                    // if the distance of two points is less than two GPS_standard_deviation, we filter out that point
                    || (distanceCalc.calcDist(prevEntry.getLat(), prevEntry.getLon(), point.getLat(), point.getLon()) > 2 * SEARCH_RANGE
                    || distanceCalc.calcDist(pointList.get(i + 1).getLat(), pointList.get(i + 1).getLon(), point.getLat(), point.getLon()) > 2 * SEARCH_RANGE)) {
                ret.add(point);
                prevEntry = point;
            } else {
                logger.debug("{}th entry has been filtered out.", i + 1);
            }
        }
        return ret;
    }

    class Candidate {

        /**
         * standard deviation of GPS device
         * @see com.graphhopper.matching.MapMatching
         */
        final double SIGMA = 50;

        /**
         * query point: the point in a query
         */
        MMPoint point;

        /**
         * candidate point of the query point
         */
        MMPoint candidatePoint;

        double probability;

        /**
         * measurement probability, in this case, it indicates how close between query points and map-matched point.
         */
        double emissionProbability;

        FastMapMatching.Candidate preCandidate;

        /**
         * point is the query point, candidatePoint is the candidate point of the query point
         * Assuming that GPS errors are Gaussian distribution
         *
         * @param point          query point
         * @param candidatePoint map matched point
         */
        Candidate(MMPoint point, MMPoint candidatePoint) {
            this.point = point;
            this.candidatePoint = candidatePoint;
            double x = GeoUtil.distance(point, candidatePoint);
            this.emissionProbability = Math.exp(-0.5 * x * x / (SIGMA * SIGMA)) / (Math.sqrt(2 * Math.PI) * SIGMA);
        }

        @Override
        public String toString() {
            return "{" + candidatePoint + ", " + probability + '}';
        }
    }

}
