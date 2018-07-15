package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.query.ShortestPathQuery;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import com.bmw.hmm.SequenceState;
import com.bmw.hmm.ViterbiAlgorithm;
import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.*;
import com.graphhopper.matching.util.HmmProbabilities;
import com.graphhopper.matching.util.TimeStep;
import com.graphhopper.routing.*;
import com.graphhopper.routing.ch.CHAlgoFactoryDecorator;
import com.graphhopper.routing.ch.PrepareContractionHierarchies;
import com.graphhopper.routing.util.*;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.CHGraph;
import com.graphhopper.storage.Graph;
import com.graphhopper.storage.index.LocationIndexTree;
import com.graphhopper.storage.index.QueryResult;
import com.graphhopper.util.*;
import com.graphhopper.util.shapes.GHPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * @author forrest0402
 * @Description
 * @date 11/30/2017
 */
public class MapMatching2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    // Penalty in m for each U-turn performed at the beginning or end of a path between two
    // subsequent candidates.
    private double uTurnDistancePenalty;

    public final Graph graph;
    private final Graph routingGraph;
    private final LocationIndexMatch locationIndex;
    private double measurementErrorSigma = 50.0;
    private double transitionProbabilityBeta = 2.0;
    private final int nodeCount;
    private DistanceCalc distanceCalc = new DistancePlaneProjection();
    private final RoutingAlgorithmFactory algoFactory;
    private final AlgorithmOptions algoOptions;
    private long _timeDuration = 0;

    //used for fast map matching
    final double MAX_DISTANCE = 1000;
    final double SEARCH_RANGE = 50;
    private ShortestPathQuery shortestPathQuery = null;
    private Map<Integer, MMPoint> towerPoints = null;
    private Map<Integer, MMPoint> allPoints = null;
    //key for MMEdge.getKey(towerPoint1, towerPoint2), for edges between the same tower points, only the shortest one will be stored
    private Map<String, MMEdge> allEdges = null;
    private Set<MMEdge> allEdgeSet;
    private RTree<MMPoint, Geometry> rTree = null;

    public void getGraph(List<MMPoint> towerPoints, List<MMEdge> allEdges) {
        towerPoints.addAll(this.towerPoints.values());
        allEdges.addAll(allEdgeSet);
    }

    public MapMatching2(GraphHopper hopper, AlgorithmOptions algoOptions, boolean preCalculation) {
        // Convert heading penalty [s] into U-turn penalty [m]
        final double PENALTY_CONVERSION_VELOCITY = 5;  // [m/s]
        final double headingTimePenalty = algoOptions.getHints().getDouble(
                Parameters.Routing.HEADING_PENALTY, Parameters.Routing.DEFAULT_HEADING_PENALTY);
        uTurnDistancePenalty = headingTimePenalty * PENALTY_CONVERSION_VELOCITY;

        this.locationIndex = new LocationIndexMatch(hopper.getGraphHopperStorage(),
                (LocationIndexTree) hopper.getLocationIndex());

        // create hints from algoOptions, so we can create the algorithm factory
        HintsMap hints = new HintsMap();
        for (Map.Entry<String, String> entry : algoOptions.getHints().toMap().entrySet()) {
            hints.put(entry.getKey(), entry.getValue());
        }

        // default is non-CH
        if (!hints.has(Parameters.CH.DISABLE)) {
            hints.put(Parameters.CH.DISABLE, true);

            if (!hopper.getCHFactoryDecorator().isDisablingAllowed())
                throw new IllegalArgumentException("Cannot disable CH. Not allowed on server side");
        }

        // TODO ugly workaround, duplicate data: hints can have 'vehicle' but algoOptions.weighting too!?
        // Similar problem in GraphHopper class
        String vehicle = hints.getVehicle();
        if (vehicle.isEmpty()) {
            if (algoOptions.hasWeighting()) {
                vehicle = algoOptions.getWeighting().getFlagEncoder().toString();
            } else {
                vehicle = hopper.getEncodingManager().fetchEdgeEncoders().get(0).toString();
            }
            hints.setVehicle(vehicle);
        }

        if (!hopper.getEncodingManager().supports(vehicle)) {
            throw new IllegalArgumentException("Vehicle " + vehicle + " unsupported. "
                    + "Supported are: " + hopper.getEncodingManager());
        }

        algoFactory = hopper.getAlgorithmFactory(hints);

        Weighting weighting;
        CHAlgoFactoryDecorator chFactoryDecorator = hopper.getCHFactoryDecorator();
        boolean forceFlexibleMode = hints.getBool(Parameters.CH.DISABLE, false);
        if (chFactoryDecorator.isEnabled() && !forceFlexibleMode) {
            if (!(algoFactory instanceof PrepareContractionHierarchies)) {
                throw new IllegalStateException("Although CH was enabled a non-CH algorithm "
                        + "factory was returned " + algoFactory);
            }

            weighting = ((PrepareContractionHierarchies) algoFactory).getWeighting();
            this.routingGraph = hopper.getGraphHopperStorage().getGraph(CHGraph.class, weighting);
        } else {
            weighting = algoOptions.hasWeighting()
                    ? algoOptions.getWeighting()
                    : new FastestWeighting(hopper.getEncodingManager().getEncoder(vehicle),
                    algoOptions.getHints());
            this.routingGraph = hopper.getGraphHopperStorage();
        }

        this.graph = hopper.getGraphHopperStorage();
        this.algoOptions = AlgorithmOptions.start(algoOptions).weighting(weighting).build();
        this.nodeCount = routingGraph.getNodes();

        if (preCalculation)
            precomputeShortestPath(hopper);
    }

    public void setDistanceCalc(DistanceCalc distanceCalc) {
        this.distanceCalc = distanceCalc;
    }

    /**
     * Beta parameter of the exponential distribution for modeling transition
     * probabilities.
     */
    public void setTransitionProbabilityBeta(double transitionProbabilityBeta) {
        this.transitionProbabilityBeta = transitionProbabilityBeta;
    }

    /**
     * Standard deviation of the normal distribution [m] used for modeling the
     * GPS error.
     */
    public void setMeasurementErrorSigma(double measurementErrorSigma) {
        this.measurementErrorSigma = measurementErrorSigma;
    }

    private void precomputeShortestPath(GraphHopper hopper) {
        logger.info("Enter - precomputeShortestPath");
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
        File file = new File("ShortestPathQuery.ser");
        if (file.exists() && file == null) {
            logger.info("precompute shortest path using ShortestPathQuery.ser");
            String content = "";
            //this.shortestPathQuery = JSON.parseObject(content, ShortestPathQuery.class);

//            try (FileInputStream fin = new FileInputStream(file);
//                 ObjectInputStream in = new ObjectInputStream(fin)) {
//                this.shortestPathQuery = (ShortestPathQuery) in.readObject();
//            } catch (FileNotFoundException e) {
//                logger.error("{}", e);
//            } catch (IOException e) {
//                logger.error("{}", e);
//            } catch (Exception e) {
//                logger.error("{}", e);
//            }

        } else {
            logger.info("precompute shortest path using Dijkstra (SPFA)");
            this.shortestPathQuery = new ShortestPathQuery();
            Dijkstra dijkstra = new Dijkstra(allPoints, allEdges, shortestPathQuery);
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

//            try {
//                JSON.writeJSONString(new FileOutputStream(file), this.shortestPathQuery);
//            } catch (IOException e) {
//                logger.error("{}", e);
//            } catch (Exception e) {
//                logger.error("{}", e);
//            }

//            try (FileOutputStream fout = new FileOutputStream(file);
//                 ObjectOutputStream out = new ObjectOutputStream(fout)) {
//                out.writeObject(this.shortestPathQuery);
//                out.flush();
//            } catch (FileNotFoundException e) {
//                logger.error("{}", e);
//            } catch (IOException e) {
//                logger.error("{}", e);
//            } catch (Exception e) {
//                logger.error("{}", e);
//            }
        }
        logger.info("Exit - precomputeShortestPath. There are {} edges, {} points including {} tower points.", allEdges.size(), allPoints.size(), towerPoints.size());
    }

    private void normalization(List<Candidate> candidates) {
        double sumP = 0.0, sumEmissionP = 0.0;
        for (Candidate candidate : candidates) {
            sumP += candidate.probability;
            //sumEmissionP += candidate.emissionProbability;
        }
        if (sumP != 0.0) {
            for (Candidate candidate : candidates) {
                candidate.probability /= sumP;
                //candidate.emissionProbability /= sumEmissionP;
            }
        }
    }

    /**
     * Forrest
     *
     * @param pointList
     * @return
     */
    public double runMMPoint(List<MMPoint> pointList, List<MMPoint> pathPoints, List<MMEdge> pathEdges) {

        pointList = filterMMPoints(pointList);

        if (pointList.size() < 2) {
            logger.error("Too few coordinates in input file ("
                    + pointList.size() + "). Correct format?");
            throw new IllegalStateException("the road is broken");
        }

        // find candidates for each candidate point
        List<List<Candidate>> candidateSet = new ArrayList<>();
        for (MMPoint point : pointList) {

            final List<Candidate> candidates = new LinkedList<>();
            double delta = 0;

            //if rtree cannot retrieve any point within the SEARCH_RANGE, then expand the range until find candidate point
            while (candidates.size() == 0) {
                rx.Observable<Entry<MMPoint, Geometry>> results = rTree.search(Geometries.rectangleGeographic(
                        GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -(SEARCH_RANGE + delta)),
                        GeoUtil.increaseLatitude(point.getLat(), -(SEARCH_RANGE + delta)),
                        GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), (SEARCH_RANGE + delta)),
                        GeoUtil.increaseLatitude(point.getLat(), (SEARCH_RANGE + delta)))
                );

                results.forEach(entry -> {
                    Candidate candidate = new Candidate(point, entry.value());
                    candidates.add(candidate);
                });

                delta += 10;
            }
            //normalize
            double sumEmissionProb = 0;
            for (Candidate candidate : candidates) {
                sumEmissionProb += candidate.emissionProbability;
            }
            for (Candidate candidate : candidates) {
                candidate.emissionProbability /= sumEmissionProb;
            }
            candidateSet.add(candidates);
        }
//        logger.info("show candidates");
//        int candidateNum = 0;
//        for (List<Candidate> candidates : candidateSet) {
//            StringBuilder stringBuilder = new StringBuilder();
//            for (Candidate candidate : candidates) {
//                stringBuilder.append("(").append(candidate.candidatePoint.getLat())
//                        .append(", ")
//                        .append(candidate.candidatePoint.getLon())
//                        .append(")");
//            }
//            System.out.println((candidateNum++) + ": " + stringBuilder.toString());
//        }
        //vertibi algorithm
        List<Candidate> preCandidates = candidateSet.get(0);
        for (Candidate preCandidate : preCandidates) {
            preCandidate.probability = 0;
            for (Candidate candidate : candidateSet.get(1)) {
                double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, candidate.candidatePoint);
                double shortestPathDistance = shortestPathQuery.minDistance(preCandidate.candidatePoint, candidate.candidatePoint);
                double transitionProbability = lineDistance / shortestPathDistance;
                if (shortestPathDistance == 0) transitionProbability = 1.0;
                preCandidate.probability += preCandidate.emissionProbability * transitionProbability;
            }
        }
        normalization(preCandidates);
        for (int i = 1; i < candidateSet.size(); ++i) {
            List<Candidate> curCandidates = candidateSet.get(i);
            double maxProb = Double.MIN_VALUE;
            for (Candidate preCandidate : preCandidates) {
                for (Candidate curCandidate : curCandidates) {
                    double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, curCandidate.candidatePoint);
                    double shortestPathDistance = shortestPathQuery.minDistance(preCandidate.candidatePoint, curCandidate.candidatePoint);
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
        Candidate maxCandidate = null;
        double maxProbability = Double.MIN_VALUE;
        for (Candidate candidate : candidateSet.get(candidateSet.size() - 1)) {
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
            double len = shortestPathQuery.minDistance(prePoint, candidatePoints.get(i));
            List<MMPoint> shortestPath = shortestPathQuery.shortestPath(prePoint, candidatePoints.get(i));
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

//    /**
//     * Forrest
//     *
//     * @param gpxList
//     * @return
//     */
//    public double run(List<GPXEntry> gpxList, List<MMPoint> pathPoints, List<MMEdge> pathEdges) {
//
//        if (gpxList.size() < 2) {
//            logger.error("Too few coordinates in input file ("
//                    + gpxList.size() + "). Correct format?");
//            return Double.MAX_VALUE;
//        }
//
////        // filter the entries:
//        List<GPXEntry> filteredGPXEntries = filterGPXEntries(gpxList);
//        if (filteredGPXEntries.size() < 2) {
//            logger.error("Only " + filteredGPXEntries.size()
//                    + " filtered GPX entries (from " + gpxList.size()
//                    + "), but two or more are needed");
//            return Double.MAX_VALUE;
//        }
//        gpxList = filteredGPXEntries;
//        // find candidates for each candidate point
//        List<List<Candidate>> candidateSet = new ArrayList<>();
//        for (GPXEntry gpxEntry : gpxList) {
//
//            final MMPoint point = new MMPoint(gpxEntry.getLat(), gpxEntry.getLon());
//
//            final List<Candidate> candidates = new LinkedList<>();
//            double delta = 0;
//
//            //if rtree cannot retrieve any point within the SEARCH_RANGE, then expand the range until find candidate point
//            while (candidates.size() == 0) {
//                rx.Observable<Entry<MMPoint, Geometry>> results = rTree.search(Geometries.rectangleGeographic(
//                        GeoUtil.increaseLongtitude(gpxEntry.getLat(), gpxEntry.getLon(), -(SEARCH_RANGE + delta)),
//                        GeoUtil.increaseLatitude(gpxEntry.getLat(), -(SEARCH_RANGE + delta)),
//                        GeoUtil.increaseLongtitude(gpxEntry.getLat(), gpxEntry.getLon(), (SEARCH_RANGE + delta)),
//                        GeoUtil.increaseLatitude(gpxEntry.getLat(), (SEARCH_RANGE + delta)))
//                );
//
//                results.forEach(entry -> {
//                    Candidate candidate = new Candidate(point, entry.value());
//                    candidates.add(candidate);
//                });
//
//                delta += 10;
//            }
//            //normalize
//            double sumEmissionProb = 0;
//            for (Candidate candidate : candidates) {
//                sumEmissionProb += candidate.emissionProbability;
//            }
//            for (Candidate candidate : candidates) {
//                candidate.emissionProbability /= sumEmissionProb;
//            }
//            candidateSet.add(candidates);
//        }
////        logger.info("show candidates");
////        int candidateNum = 0;
////        for (List<Candidate> candidates : candidateSet) {
////            StringBuilder stringBuilder = new StringBuilder();
////            for (Candidate candidate : candidates) {
////                stringBuilder.append("(").append(candidate.candidatePoint.getLat())
////                        .append(", ")
////                        .append(candidate.candidatePoint.getLon())
////                        .append(")");
////            }
////            System.out.println((candidateNum++) + ": " + stringBuilder.toString());
////        }
//        //vertibi algorithm
//        List<Candidate> preCandidates = candidateSet.get(0);
//        for (Candidate preCandidate : preCandidates) {
//            preCandidate.probability = 0;
//            for (Candidate candidate : candidateSet.get(1)) {
//                double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, candidate.candidatePoint);
//                double shortestPathDistance = shortestPathQuery.minDistance(preCandidate.candidatePoint, candidate.candidatePoint);
//                double transitionProbability = lineDistance / shortestPathDistance;
//                if (shortestPathDistance == 0) transitionProbability = 1.0;
//                preCandidate.probability += preCandidate.emissionProbability * transitionProbability;
//            }
//        }
//        normalization(preCandidates);
//        for (int i = 1; i < candidateSet.size(); ++i) {
//            List<Candidate> curCandidates = candidateSet.get(i);
//            double maxProb = Double.MIN_VALUE;
//            for (Candidate preCandidate : preCandidates) {
//                for (Candidate curCandidate : curCandidates) {
//                    double lineDistance = GeoUtil.distance(preCandidate.candidatePoint, curCandidate.candidatePoint);
//                    double shortestPathDistance = shortestPathQuery.minDistance(preCandidate.candidatePoint, curCandidate.candidatePoint);
//                    double transitionProbability = lineDistance / shortestPathDistance;
//                    if (shortestPathDistance == 0) transitionProbability = 1.0;
//                    double p = preCandidate.probability * transitionProbability * curCandidate.emissionProbability;
//                    if (p > curCandidate.probability) {
//                        curCandidate.probability = p;
//                        curCandidate.preCandidate = preCandidate;
//                    }
//                    if (p > maxProb)
//                        maxProb = p;
//                }
//            }
//            preCandidates = curCandidates;
//            if (maxProb < 1e-150)
//                normalization(preCandidates);
//        }
//
//        //get the result, first find the candidate from the last candidate set with the maximum probability,
//        //then find the optimal candidate chain
//        //finally get the shortest path between two candidates
//        Candidate maxCandidate = null;
//        double maxProbability = Double.MIN_VALUE;
//        for (Candidate candidate : candidateSet.get(candidateSet.size() - 1)) {
//            if (candidate.probability > maxProbability) {
//                maxProbability = candidate.probability;
//                maxCandidate = candidate;
//            }
//        }
//        if (maxCandidate == null) {
//            logger.error("the road is broken {}", gpxList);
//            return Double.MAX_VALUE;
//        }
//        List<MMPoint> candidatePoints = new LinkedList<>();
//        List<MMPoint> resPoints = new ArrayList<>();
//        while (maxCandidate != null) {
//            candidatePoints.add(0, maxCandidate.candidatePoint);
//            maxCandidate = maxCandidate.preCandidate;
//        }
//        MMPoint prePoint = candidatePoints.get(0);
//        resPoints.add(prePoint);
//        double pathLength = 0;
//        for (int i = 1; i < candidatePoints.size(); ++i) {
//            List<MMPoint> shortestPath = shortestPathQuery.shortestPath(prePoint, candidatePoints.get(i));
//            if (shortestPath != null)
//                resPoints.addAll(shortestPath);
//            else //cannot find shortest path within 1000 m, the point is broken{
//            {
//                resPoints.add(candidatePoints.get(i));
//                logger.error("the road is broken {}", gpxList);
//                return Double.MAX_VALUE;
//            }
//            double len = shortestPathQuery.minDistance(prePoint, candidatePoints.get(i));
//            pathLength += len;
//            prePoint = candidatePoints.get(i);
//        }
//
//        //filter out pillar points excluding the start point and end point
//        int idx = 0;
//        if (pathPoints != null)
//            pathPoints.add(resPoints.get(idx++));
//        prePoint = resPoints.get(0);
//        for (; idx < resPoints.size() - 1; ++idx) {
//            if (resPoints.get(idx).isTowerPoint) {
//                if (pathPoints != null && !pathPoints.get(pathPoints.size() - 1).equals(resPoints.get(idx)))
//                    pathPoints.add(resPoints.get(idx));
//                if (pathEdges != null)
//                    pathEdges.add(allEdges.get(MMEdge.getKey(prePoint, resPoints.get(idx))));
//                prePoint = resPoints.get(idx);
//            }
//        }
//        if (pathPoints != null)
//            pathPoints.add(resPoints.get(idx));
//        return pathLength;
//    }

    private void filterTowerPoint(List<MMPoint> shortestPath) {
        if (shortestPath != null) {
            Iterator<MMPoint> iterator = shortestPath.iterator();
            while (iterator.hasNext()) {
                MMPoint point = iterator.next();
                if (!point.isTowerPoint)
                    iterator.remove();
            }
        }
    }

    /**
     * This method does the actual map matching.
     * <p>
     *
     * @param gpxList the input list with GPX points which should match to edges
     *                of the graph specified in the constructor
     */
    public MatchResult doWork(List<GPXEntry> gpxList) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        if (gpxList.size() < 2) {
            throw new IllegalArgumentException("Too few coordinates in input file ("
                    + gpxList.size() + "). Correct format?");
        }

        // filter the entries:
        List<GPXEntry> filteredGPXEntries = filterGPXEntries(gpxList);
        if (filteredGPXEntries.size() < 2) {
            throw new IllegalStateException("Only " + filteredGPXEntries.size()
                    + " filtered GPX entries (from " + gpxList.size()
                    + "), but two or more are needed");
        }
        stopWatch.stop();
        logger.info("filter time: {} ms", stopWatch.getTime());
        stopWatch.start();
        // now find each of the entries in the graph:
        final EdgeFilter edgeFilter = new DefaultEdgeFilter(algoOptions.getWeighting().getFlagEncoder());

        List<Collection<QueryResult>> queriesPerEntry =
                lookupGPXEntries(filteredGPXEntries, edgeFilter);

        // Add virtual nodes and edges to the graph so that candidates on edges can be represented
        // by virtual nodes.
        final QueryGraph queryGraph = new QueryGraph(routingGraph).setUseEdgeExplorerCache(true);
        List<QueryResult> allQueryResults = new ArrayList<>();
        for (Collection<QueryResult> qrs : queriesPerEntry)
            allQueryResults.addAll(qrs);
        queryGraph.lookup(allQueryResults);

        // Different QueryResults can have the same tower node as their closest node.
        // Hence, we now dedupe the query results of each GPX entry by their closest node (#91).
        // This must be done after calling queryGraph.lookup() since this replaces some of the
        // QueryResult nodes with virtual nodes. Virtual nodes are not deduped since there is at
        // most one QueryResult per edge and virtual nodes are inserted into the middle of an edge.
        // Reducing the number of QueryResults improves performance since less shortest/fastest
        // routes need to be computed.
        queriesPerEntry = deduplicateQueryResultsByClosestNode(queriesPerEntry);
        stopWatch.stop();
        logger.info("find candidate time: {} ms", stopWatch.getTime());
        stopWatch.start();
        logger.debug("================= Query results =================");
        int i = 1;
        for (Collection<QueryResult> entries : queriesPerEntry) {
            logger.debug("Query results for GPX entry {}", i++);
            for (QueryResult qr : entries) {
                logger.debug("Node id: {}, virtual: {}, snapped on: {}, pos: {},{}, "
                                + "query distance: {}", qr.getClosestNode(),
                        isVirtualNode(qr.getClosestNode()), qr.getSnappedPosition(),
                        qr.getSnappedPoint().getLat(), qr.getSnappedPoint().getLon(),
                        qr.getQueryDistance());
            }
        }

        // Creates candidates from the QueryResults of all GPX entries (a candidate is basically a
        // QueryResult + direction).
        List<TimeStep<GPXExtension, GPXEntry, Path>> timeSteps =
                createTimeSteps(filteredGPXEntries, queriesPerEntry, queryGraph);
        logger.debug("=============== Time steps ===============");
        i = 1;
        for (TimeStep<GPXExtension, GPXEntry, Path> ts : timeSteps) {
            logger.debug("Candidates for time step {}", i++);
            for (GPXExtension candidate : ts.candidates) {
                logger.debug(candidate.toString());
            }
        }

        // Compute the most likely sequence of map matching candidates:
        List<SequenceState<GPXExtension, GPXEntry, Path>> seq = computeViterbiSequence(timeSteps,
                gpxList.size(), queryGraph);

        stopWatch.stop();
        logger.info("Viterbi time: {} ms", stopWatch.getTime());
        stopWatch.start();
        logger.debug("=============== Viterbi results =============== ");
        i = 1;
        for (SequenceState<GPXExtension, GPXEntry, Path> ss : seq) {
            logger.debug("{}: {}, path: {}", i, ss.state,
                    ss.transitionDescriptor != null ? ss.transitionDescriptor.calcEdges() : null);
            i++;
        }

        // finally, extract the result:
        final EdgeExplorer explorer = queryGraph.createEdgeExplorer(edgeFilter);

        // Needs original gpxList to compute stats.
        MatchResult matchResult = computeMatchResult(seq, gpxList, queriesPerEntry, explorer);

        logger.debug("=============== Matched real edges =============== ");
        i = 1;
        for (EdgeMatch em : matchResult.getEdgeMatches()) {
            logger.debug("{}: {}", i, em.getEdgeState());
            i++;
        }

        return matchResult;
    }

    /**
     * Filters GPX entries to only those which will be used for map matching (i.e. those which
     * are separated by at least 2 * measurementErrorSigman
     */
    private List<GPXEntry> filterGPXEntries(List<GPXEntry> gpxList) {
        List<GPXEntry> filtered = new ArrayList<>();
        GPXEntry prevEntry = null;
        int last = gpxList.size() - 1;
        for (int i = 0; i <= last; i++) {
            GPXEntry gpxEntry = gpxList.get(i);
            if (i == 0 || i == last || distanceCalc.calcDist(
                    prevEntry.getLat(), prevEntry.getLon(),
                    gpxEntry.getLat(), gpxEntry.getLon()) > 2 * measurementErrorSigma) {
                filtered.add(gpxEntry);
                prevEntry = gpxEntry;
            } else {
                logger.debug("Filter out GPX entry: {}", i + 1);
            }
        }
        return filtered;
    }

    private List<MMPoint> filterMMPoints(List<MMPoint> pointList) {
        List<MMPoint> filtered = new ArrayList<>();
        MMPoint prevEntry = null;
        int last = pointList.size() - 1;
        for (int i = 0; i <= last; i++) {
            MMPoint point = pointList.get(i);
            if (i == 0 || i == last
                    || (distanceCalc.calcDist(prevEntry.getLat(), prevEntry.getLon(), point.getLat(), point.getLon()) > 2 * SEARCH_RANGE
                    || distanceCalc.calcDist(pointList.get(i + 1).getLat(), pointList.get(i + 1).getLon(), point.getLat(), point.getLon()) > 2 * SEARCH_RANGE)) {
                filtered.add(point);
                prevEntry = point;
            } else {
                logger.debug("Filter out GPX entry: {}", i + 1);
            }
        }
        return filtered;
    }

    public static void printPoints(List<MMPoint> pointList) {
        System.out.print("https://graphhopper.com/maps/?");
        for (MMPoint mmPoint : pointList) {
            System.out.print("&point=" + mmPoint.getLat() + "%2C" + mmPoint.getLon());
        }
        System.out.println("");
    }

    /**
     * Find the possible locations (edges) of each GPXEntry in the graph.
     */
    private List<Collection<QueryResult>> lookupGPXEntries(List<GPXEntry> gpxList,
                                                           EdgeFilter edgeFilter) {

        final List<Collection<QueryResult>> gpxEntryLocations = new ArrayList<>();
        for (GPXEntry gpxEntry : gpxList) {
            final List<QueryResult> queryResults = locationIndex.findNClosest(
                    gpxEntry.lat, gpxEntry.lon, edgeFilter, measurementErrorSigma);
            gpxEntryLocations.add(queryResults);
        }
        return gpxEntryLocations;
    }

    private List<Collection<QueryResult>> deduplicateQueryResultsByClosestNode(
            List<Collection<QueryResult>> queriesPerEntry) {
        final List<Collection<QueryResult>> result = new ArrayList<>(queriesPerEntry.size());

        for (Collection<QueryResult> queryResults : queriesPerEntry) {
            final Map<Integer, QueryResult> dedupedQueryResults = new HashMap<>();
            for (QueryResult qr : queryResults) {
                dedupedQueryResults.put(qr.getClosestNode(), qr);
            }
            result.add(dedupedQueryResults.values());
        }
        return result;
    }

    /**
     * Creates TimeSteps with candidates for the GPX entries but does not create emission or
     * transition probabilities. Creates directed candidates for virtual nodes and undirected
     * candidates for real nodes.
     */
    private List<TimeStep<GPXExtension, GPXEntry, Path>> createTimeSteps(
            List<GPXEntry> filteredGPXEntries, List<Collection<QueryResult>> queriesPerEntry,
            QueryGraph queryGraph) {
        final int n = filteredGPXEntries.size();
        if (queriesPerEntry.size() != n) {
            throw new IllegalArgumentException(
                    "filteredGPXEntries and queriesPerEntry must have same size.");
        }

        final List<TimeStep<GPXExtension, GPXEntry, Path>> timeSteps = new ArrayList<>();
        for (int i = 0; i < n; i++) {

            GPXEntry gpxEntry = filteredGPXEntries.get(i);
            final Collection<QueryResult> queryResults = queriesPerEntry.get(i);

            List<GPXExtension> candidates = new ArrayList<>();
            for (QueryResult qr : queryResults) {
                int closestNode = qr.getClosestNode();
                if (queryGraph.isVirtualNode(closestNode)) {
                    // get virtual edges:
                    List<VirtualEdgeIteratorState> virtualEdges = new ArrayList<>();
                    EdgeIterator iter = queryGraph.createEdgeExplorer().setBaseNode(closestNode);
                    while (iter.next()) {
                        if (!queryGraph.isVirtualEdge(iter.getEdge())) {
                            throw new RuntimeException("Virtual nodes must only have virtual edges "
                                    + "to adjacent nodes.");
                        }
                        virtualEdges.add((VirtualEdgeIteratorState)
                                queryGraph.getEdgeIteratorState(iter.getEdge(), iter.getAdjNode()));
                    }
                    if (virtualEdges.size() != 2) {
                        throw new RuntimeException("Each virtual node must have exactly 2 "
                                + "virtual edges (reverse virtual edges are not returned by the "
                                + "EdgeIterator");
                    }

                    // Create a directed candidate for each of the two possible directions through
                    // the virtual node. This is needed to penalize U-turns at virtual nodes
                    // (see also #51). We need to add candidates for both directions because
                    // we don't know yet which is the correct one. This will be figured
                    // out by the Viterbi algorithm.
                    //
                    // Adding further candidates to explicitly allow U-turns through setting
                    // incomingVirtualEdge==outgoingVirtualEdge doesn't make sense because this
                    // would actually allow to perform a U-turn without a penalty by going to and
                    // from the virtual node through the other virtual edge or its reverse edge.
                    VirtualEdgeIteratorState e1 = virtualEdges.get(0);
                    VirtualEdgeIteratorState e2 = virtualEdges.get(1);
                    for (int j = 0; j < 2; j++) {
                        // get favored/unfavored edges:
                        VirtualEdgeIteratorState incomingVirtualEdge = j == 0 ? e1 : e2;
                        VirtualEdgeIteratorState outgoingVirtualEdge = j == 0 ? e2 : e1;
                        // create candidate
                        QueryResult vqr = new QueryResult(qr.getQueryPoint().lat, qr.getQueryPoint().lon);
                        vqr.setQueryDistance(qr.getQueryDistance());
                        vqr.setClosestNode(qr.getClosestNode());
                        vqr.setWayIndex(qr.getWayIndex());
                        vqr.setSnappedPosition(qr.getSnappedPosition());
                        vqr.setClosestEdge(qr.getClosestEdge());
                        vqr.calcSnappedPoint(distanceCalc);
                        GPXExtension candidate = new GPXExtension(gpxEntry, vqr, incomingVirtualEdge,
                                outgoingVirtualEdge);
                        candidates.add(candidate);
                    }
                } else {
                    // Create an undirected candidate for the real node.
                    GPXExtension candidate = new GPXExtension(gpxEntry, qr);
                    candidates.add(candidate);
                }
            }

            final TimeStep<GPXExtension, GPXEntry, Path> timeStep = new TimeStep<>(gpxEntry, candidates);
            timeSteps.add(timeStep);
        }
        return timeSteps;
    }

    /**
     * Computes the most likely candidate sequence for the GPX entries.
     */
    private List<SequenceState<GPXExtension, GPXEntry, Path>> computeViterbiSequence(
            List<TimeStep<GPXExtension, GPXEntry, Path>> timeSteps, int originalGpxEntriesCount,
            QueryGraph queryGraph) {
        this._timeDuration = 0;
        final HmmProbabilities probabilities
                = new HmmProbabilities(measurementErrorSigma, transitionProbabilityBeta);
        final ViterbiAlgorithm<GPXExtension, GPXEntry, Path> viterbi = new ViterbiAlgorithm<>();

        logger.debug("\n=============== Paths ===============");
        int timeStepCounter = 0;
        TimeStep<GPXExtension, GPXEntry, Path> prevTimeStep = null;
        int i = 1;
        for (TimeStep<GPXExtension, GPXEntry, Path> timeStep : timeSteps) {
            logger.debug("\nPaths to time step {}", i++);
            computeEmissionProbabilities(timeStep, probabilities);

            if (prevTimeStep == null) {
                viterbi.startWithInitialObservation(timeStep.observation, timeStep.candidates,
                        timeStep.emissionLogProbabilities);
            } else {
                computeTransitionProbabilities(prevTimeStep, timeStep, probabilities, queryGraph);
                viterbi.nextStep(timeStep.observation, timeStep.candidates,
                        timeStep.emissionLogProbabilities, timeStep.transitionLogProbabilities,
                        timeStep.roadPaths);
            }
            if (viterbi.isBroken()) {
                String likelyReasonStr = "";
                if (prevTimeStep != null) {
                    GPXEntry prevGPXE = prevTimeStep.observation;
                    GPXEntry gpxe = timeStep.observation;
                    double dist = distanceCalc.calcDist(prevGPXE.lat, prevGPXE.lon,
                            gpxe.lat, gpxe.lon);
                    if (dist > 2000) {
                        likelyReasonStr = "Too long distance to previous measurement? "
                                + Math.round(dist) + "m, ";
                    }
                }

                throw new IllegalArgumentException("Sequence is broken for submitted track at time step "
                        + timeStepCounter + " (" + originalGpxEntriesCount + " points). "
                        + likelyReasonStr + "observation:" + timeStep.observation + ", "
                        + timeStep.candidates.size() + " candidates: "
                        + getSnappedCandidates(timeStep.candidates)
                        + ". If a match is expected consider increasing max_visited_nodes.");
            }

            timeStepCounter++;
            prevTimeStep = timeStep;
        }
        logger.info("computeTransitionProbabilities: {} ms", this._timeDuration / 1e6);
        return viterbi.computeMostLikelySequence();
    }

    private void computeEmissionProbabilities(TimeStep<GPXExtension, GPXEntry, Path> timeStep,
                                              HmmProbabilities probabilities) {
        for (GPXExtension candidate : timeStep.candidates) {
            // road distance difference in meters
            final double distance = candidate.getQueryResult().getQueryDistance();
            timeStep.addEmissionLogProbability(candidate,
                    probabilities.emissionLogProbability(distance));
        }
    }

    private void computeTransitionProbabilities(TimeStep<GPXExtension, GPXEntry, Path> prevTimeStep,
                                                TimeStep<GPXExtension, GPXEntry, Path> timeStep,
                                                HmmProbabilities probabilities,
                                                QueryGraph queryGraph) {
        long startTime = System.nanoTime();

        final double linearDistance = distanceCalc.calcDist(prevTimeStep.observation.lat,
                prevTimeStep.observation.lon, timeStep.observation.lat, timeStep.observation.lon);

        // time difference in seconds
        final double timeDiff
                = (timeStep.observation.getTime() - prevTimeStep.observation.getTime()) / 1000.0;
        logger.debug("Time difference: {} s", timeDiff);

        for (GPXExtension from : prevTimeStep.candidates) {
            for (GPXExtension to : timeStep.candidates) {
                // enforce heading if required:
                if (from.isDirected()) {
                    // Make sure that the path starting at the "from" candidate goes through
                    // the outgoing edge.
                    queryGraph.unfavorVirtualEdgePair(from.getQueryResult().getClosestNode(),
                            from.getIncomingVirtualEdge().getEdge());
                }
                if (to.isDirected()) {
                    // Make sure that the path ending at "to" candidate goes through
                    // the incoming edge.
                    queryGraph.unfavorVirtualEdgePair(to.getQueryResult().getClosestNode(),
                            to.getOutgoingVirtualEdge().getEdge());
                }

                // Need to create a new routing algorithm for every routing.
                RoutingAlgorithm algo = algoFactory.createAlgo(queryGraph, algoOptions);

                final Path path = algo.calcPath(from.getQueryResult().getClosestNode(),
                        to.getQueryResult().getClosestNode());

                if (path.isFound()) {
                    timeStep.addRoadPath(from, to, path);

                    // The router considers unfavored virtual edges using edge penalties
                    // but this is not reflected in the path distance. Hence, we need to adjust the
                    // path distance accordingly.
                    final double penalizedPathDistance = penalizedPathDistance(path,
                            queryGraph.getUnfavoredVirtualEdges());

                    logger.debug("Path from: {}, to: {}, penalized path length: {}",
                            from, to, penalizedPathDistance);

                    final double transitionLogProbability = probabilities
                            .transitionLogProbability(penalizedPathDistance, linearDistance);
                    timeStep.addTransitionLogProbability(from, to, transitionLogProbability);
                } else {
                    logger.debug("No path found for from: {}, to: {}", from, to);
                }
                queryGraph.clearUnfavoredStatus();

            }
        }
        this._timeDuration += System.nanoTime() - startTime;
    }

    /**
     * Returns the path length plus a penalty if the starting/ending edge is unfavored.
     */
    private double penalizedPathDistance(Path path,
                                         Set<EdgeIteratorState> penalizedVirtualEdges) {
        double totalPenalty = 0;

        // Unfavored edges in the middle of the path should not be penalized because we are
        // only concerned about the direction at the start/end.
        final List<EdgeIteratorState> edges = path.calcEdges();
        if (!edges.isEmpty()) {
            if (penalizedVirtualEdges.contains(edges.get(0))) {
                totalPenalty += uTurnDistancePenalty;
            }
        }
        if (edges.size() > 1) {
            if (penalizedVirtualEdges.contains(edges.get(edges.size() - 1))) {
                totalPenalty += uTurnDistancePenalty;
            }
        }
        return path.getDistance() + totalPenalty;
    }

    private MatchResult computeMatchResult(List<SequenceState<GPXExtension, GPXEntry, Path>> seq,
                                           List<GPXEntry> gpxList,
                                           List<Collection<QueryResult>> queriesPerEntry,
                                           EdgeExplorer explorer) {
        final Map<String, EdgeIteratorState> virtualEdgesMap = createVirtualEdgesMap(
                queriesPerEntry, explorer);
        MatchResult matchResult = computeMatchedEdges(seq, virtualEdgesMap);
        computeGpxStats(gpxList, matchResult);

        return matchResult;
    }

    private MatchResult computeMatchedEdges(List<SequenceState<GPXExtension, GPXEntry, Path>> seq,
                                            Map<String, EdgeIteratorState> virtualEdgesMap) {
        List<EdgeMatch> edgeMatches = new ArrayList<>();
        double distance = 0.0;
        long time = 0;
        EdgeIteratorState currentEdge = null;
        List<GPXExtension> gpxExtensions = new ArrayList<>();
        GPXExtension queryResult = seq.get(0).state;
        gpxExtensions.add(queryResult);
        for (int j = 1; j < seq.size(); j++) {
            queryResult = seq.get(j).state;
            Path path = seq.get(j).transitionDescriptor;
            distance += path.getDistance();
            time += path.getTime();
            for (EdgeIteratorState edgeIteratorState : path.calcEdges()) {
                EdgeIteratorState directedRealEdge = resolveToRealEdge(virtualEdgesMap,
                        edgeIteratorState);
                if (directedRealEdge == null) {
                    throw new RuntimeException("Did not find real edge for "
                            + edgeIteratorState.getEdge());
                }
                if (currentEdge == null || !equalEdges(directedRealEdge, currentEdge)) {
                    if (currentEdge != null) {
                        EdgeMatch edgeMatch = new EdgeMatch(currentEdge, gpxExtensions);
                        edgeMatches.add(edgeMatch);
                        gpxExtensions = new ArrayList<>();
                    }
                    currentEdge = directedRealEdge;
                }
            }
            gpxExtensions.add(queryResult);
        }
        if (edgeMatches.isEmpty()) {
            throw new IllegalArgumentException("No edge matches found for submitted track. Too short? Sequence size " + seq.size());
        }
        EdgeMatch lastEdgeMatch = edgeMatches.get(edgeMatches.size() - 1);
        if (!gpxExtensions.isEmpty() && !equalEdges(currentEdge, lastEdgeMatch.getEdgeState())) {
            edgeMatches.add(new EdgeMatch(currentEdge, gpxExtensions));
        } else {
            lastEdgeMatch.getGpxExtensions().addAll(gpxExtensions);
        }
        MatchResult matchResult = new MatchResult(edgeMatches);
        matchResult.setMatchMillis(time);
        matchResult.setMatchLength(distance);
        return matchResult;
    }

    /**
     * Calculate GPX stats to determine quality of matching.
     */
    private void computeGpxStats(List<GPXEntry> gpxList, MatchResult matchResult) {
        double gpxLength = 0;
        GPXEntry prevEntry = gpxList.get(0);
        for (int i = 1; i < gpxList.size(); i++) {
            GPXEntry entry = gpxList.get(i);
            gpxLength += distanceCalc.calcDist(prevEntry.lat, prevEntry.lon, entry.lat, entry.lon);
            prevEntry = entry;
        }

        long gpxMillis = gpxList.get(gpxList.size() - 1).getTime() - gpxList.get(0).getTime();
        matchResult.setGPXEntriesMillis(gpxMillis);
        matchResult.setGPXEntriesLength(gpxLength);
    }

    private boolean equalEdges(EdgeIteratorState edge1, EdgeIteratorState edge2) {
        return edge1.getEdge() == edge2.getEdge()
                && edge1.getBaseNode() == edge2.getBaseNode()
                && edge1.getAdjNode() == edge2.getAdjNode();
    }

    private EdgeIteratorState resolveToRealEdge(Map<String, EdgeIteratorState> virtualEdgesMap,
                                                EdgeIteratorState edgeIteratorState) {
        if (isVirtualNode(edgeIteratorState.getBaseNode())
                || isVirtualNode(edgeIteratorState.getAdjNode())) {
            return virtualEdgesMap.get(virtualEdgesMapKey(edgeIteratorState));
        } else {
            return edgeIteratorState;
        }
    }

    private boolean isVirtualNode(int node) {
        return node >= nodeCount;
    }

    /**
     * Returns a map where every virtual edge maps to its real edge with correct orientation.
     */
    private Map<String, EdgeIteratorState> createVirtualEdgesMap(
            List<Collection<QueryResult>> queriesPerEntry, EdgeExplorer explorer) {
        // TODO For map key, use the traversal key instead of string!
        Map<String, EdgeIteratorState> virtualEdgesMap = new HashMap<>();
        for (Collection<QueryResult> queryResults : queriesPerEntry) {
            for (QueryResult qr : queryResults) {
                if (isVirtualNode(qr.getClosestNode())) {
                    EdgeIterator iter = explorer.setBaseNode(qr.getClosestNode());
                    while (iter.next()) {
                        int node = traverseToClosestRealAdj(explorer, iter);
                        if (node == qr.getClosestEdge().getAdjNode()) {
                            virtualEdgesMap.put(virtualEdgesMapKey(iter),
                                    qr.getClosestEdge().detach(false));
                            virtualEdgesMap.put(reverseVirtualEdgesMapKey(iter),
                                    qr.getClosestEdge().detach(true));
                        } else if (node == qr.getClosestEdge().getBaseNode()) {
                            virtualEdgesMap.put(virtualEdgesMapKey(iter),
                                    qr.getClosestEdge().detach(true));
                            virtualEdgesMap.put(reverseVirtualEdgesMapKey(iter),
                                    qr.getClosestEdge().detach(false));
                        } else {
                            throw new RuntimeException();
                        }
                    }
                }
            }
        }
        return virtualEdgesMap;
    }

    private String virtualEdgesMapKey(EdgeIteratorState iter) {
        return iter.getBaseNode() + "-" + iter.getEdge() + "-" + iter.getAdjNode();
    }

    private String reverseVirtualEdgesMapKey(EdgeIteratorState iter) {
        return iter.getAdjNode() + "-" + iter.getEdge() + "-" + iter.getBaseNode();
    }

    private int traverseToClosestRealAdj(EdgeExplorer explorer, EdgeIteratorState edge) {
        if (!isVirtualNode(edge.getAdjNode())) {
            return edge.getAdjNode();
        }

        EdgeIterator iter = explorer.setBaseNode(edge.getAdjNode());
        while (iter.next()) {
            if (iter.getAdjNode() != edge.getBaseNode()) {
                return traverseToClosestRealAdj(explorer, iter);
            }
        }
        throw new IllegalStateException("Cannot find adjacent edge " + edge);
    }

    private String getSnappedCandidates(Collection<GPXExtension> candidates) {
        String str = "";
        for (GPXExtension gpxe : candidates) {
            if (!str.isEmpty()) {
                str += ", ";
            }
            str += "distance: " + gpxe.getQueryResult().getQueryDistance() + " to "
                    + gpxe.getQueryResult().getSnappedPoint();
        }
        return "[" + str + "]";
    }

    private void printMinDistances(List<TimeStep<GPXExtension, GPXEntry, Path>> timeSteps) {
        TimeStep<GPXExtension, GPXEntry, Path> prevStep = null;
        int index = 0;
        for (TimeStep<GPXExtension, GPXEntry, Path> ts : timeSteps) {
            if (prevStep != null) {
                double dist = distanceCalc.calcDist(
                        prevStep.observation.lat, prevStep.observation.lon,
                        ts.observation.lat, ts.observation.lon);
                double minCand = Double.POSITIVE_INFINITY;
                for (GPXExtension prevGPXE : prevStep.candidates) {
                    for (GPXExtension gpxe : ts.candidates) {
                        GHPoint psp = prevGPXE.getQueryResult().getSnappedPoint();
                        GHPoint sp = gpxe.getQueryResult().getSnappedPoint();
                        double tmpDist = distanceCalc.calcDist(psp.lat, psp.lon, sp.lat, sp.lon);
                        if (tmpDist < minCand) {
                            minCand = tmpDist;
                        }
                    }
                }
                logger.debug(index + ": " + Math.round(dist) + "m, minimum candidate: "
                        + Math.round(minCand) + "m");
                index++;
            }

            prevStep = ts;
        }
    }

    private static class MapMatchedPath extends Path {

        public MapMatchedPath(Graph graph, Weighting weighting) {
            super(graph, weighting);
        }

        @Override
        public Path setFromNode(int from) {
            return super.setFromNode(from);
        }

        @Override
        public void processEdge(int edgeId, int adjNode, int prevEdgeId) {
            super.processEdge(edgeId, adjNode, prevEdgeId);
        }
    }

    public Path calcPath(MatchResult mr) {
        MapMatching2.MapMatchedPath p = new MapMatching2.MapMatchedPath(graph, algoOptions.getWeighting());
        if (!mr.getEdgeMatches().isEmpty()) {
            int prevEdge = EdgeIterator.NO_EDGE;
            p.setFromNode(mr.getEdgeMatches().get(0).getEdgeState().getBaseNode());
            for (EdgeMatch em : mr.getEdgeMatches()) {
                p.processEdge(em.getEdgeState().getEdge(), em.getEdgeState().getAdjNode(), prevEdge);
                prevEdge = em.getEdgeState().getEdge();
            }

            p.setFound(true);

            return p;
        } else {
            return p;
        }
    }

    class Candidate {

        final double SIGMA = 50;

        /**
         * point is the query point, candidatePoint is the candidate point of the query point
         * Assuming that GPS errors are Gaussian distribution
         *
         * @param point
         * @param candidatePoint
         */
        public Candidate(MMPoint point, MMPoint candidatePoint) {
            this.point = point;
            this.candidatePoint = candidatePoint;
            double x = GeoUtil.distance(point, candidatePoint);
            this.emissionProbability = Math.exp(-0.5 * x * x / (SIGMA * SIGMA)) / (Math.sqrt(2 * Math.PI) * SIGMA);
        }

        /**
         * query point: the point in a query
         */
        public MMPoint point;

        /**
         * candidate point of the query point
         */
        public MMPoint candidatePoint;

        public double probability;

        public double emissionProbability; // TransitionProbabilities

        public Candidate preCandidate;

        @Override
        public String toString() {
            return "{" + candidatePoint + ", " + probability + '}';
        }
    }

}
