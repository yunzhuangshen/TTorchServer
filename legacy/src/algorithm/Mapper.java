package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.AppEngine;
import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.task.PortoTrajectoryFormatTask;
import au.edu.rmit.trajectory.similarity.task.TrajectoryConvertTask;
import au.edu.rmit.trajectory.similarity.task.formatter.LineFormatter;
import com.graphhopper.GraphHopper;
import com.graphhopper.matching.EdgeMatch;
import com.graphhopper.matching.MapMatching;
import com.graphhopper.matching.MatchResult;
import com.graphhopper.reader.osm.GraphHopperOSM;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.util.CarFlagEncoder;
import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.routing.weighting.Weighting;
import com.graphhopper.storage.Graph;
import com.graphhopper.util.GPXEntry;
import com.graphhopper.util.Parameters;
import com.graphhopper.util.PointList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

/**
 * wrapper for GraphHopper mapmatching API.
 *
 * @author forrest0402
 * @date 11/16/2017
 */
@Component
public class Mapper {

    private static Logger logger = LoggerFactory.getLogger(Mapper.class);

    private MapMatching mapMatching = null;

    private MapMatching2 MapMatching2 = null;

    private final int THREAD_NUM = 10;

    private final int BLOCKING_QUEUE_SIZE = 1000000;

    private boolean hasRead = false;

    private GraphHopper hopper = null;

    public GraphHopper getHopper() {
        return this.hopper;
    }

    public void clear() {
        this.hopper = null;
        this.MapMatching2 = null;
        this.mapMatching = null;
        hasRead = false;
    }

    /**
     * First, the subroutine loads osm data file and generate a virtual graph( instance of Graph Class) from osm dataset,
     * which contains all the points and edges in this area.
     * Second, construct a instance of type MapMatching.
     *
     * @param OSMPath the path to .osm file
     * @param targetPath the directory for which output goes
     * */
    public void GraphHopperReadPDF(String OSMPath, String targetPath) {
        if (hasRead)
            return;
        hasRead = true;
        hopper = new GraphHopperOSM();
        hopper.setDataReaderFile(OSMPath);
        hopper.setGraphHopperLocation(targetPath);
        CarFlagEncoder encoder = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(encoder));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();

        // create MapMatching object, can and should be shared accross threads
        String algorithm = Parameters.Algorithms.ASTAR_BI;
        Weighting weighting = new FastestWeighting(encoder);
        AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
        this.mapMatching = new MapMatching(hopper, algoOptions);
    }

    /**
     * @param OSMPath
     * @param targetPath
     */
    public void readPBF(String OSMPath, String targetPath) {
        if (hasRead)
            return;
        hasRead = true;
        hopper = new GraphHopperOSM();
        hopper.setDataReaderFile(OSMPath);
        hopper.setGraphHopperLocation(targetPath);
        CarFlagEncoder encoder = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(encoder));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();

        // create MapMatching object, can and should be shared accross threads
        String algorithm = Parameters.Algorithms.ASTAR_BI;
        Weighting weighting = new FastestWeighting(encoder);
        AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
        this.MapMatching2 = new MapMatching2(hopper, algoOptions, true);
    }

    /**
     * First, the subroutine loads osm data file and generate a virtual graph( instance of Graph Class) from osm dataset,
     * which contains all the points and edges in this area.
     * Second, construct a instance of MapMatching2
     *
     * @param OSMPath the path to .osm file
     * @param targetPath the directory for which output goes
     * @return a instance of Graph.
     * */
    public Graph getGraph(String OSMPath, String targetPath) {
        if (hasRead)
            return this.MapMatching2.graph;
        hasRead = true;
        hopper = new GraphHopperOSM();
        hopper.setDataReaderFile(OSMPath);
        hopper.setGraphHopperLocation(targetPath);
        CarFlagEncoder encoder = new CarFlagEncoder();
        hopper.setEncodingManager(new EncodingManager(encoder));
        hopper.getCHFactoryDecorator().setEnabled(false);
        hopper.importOrLoad();

        // create MapMatching object, can and should be shared across threads
        String algorithm = Parameters.Algorithms.ASTAR_BI;
        Weighting weighting = new FastestWeighting(encoder);
        AlgorithmOptions algoOptions = new AlgorithmOptions(algorithm, weighting);
        this.MapMatching2 = new MapMatching2(hopper, algoOptions, false);
        return this.MapMatching2.graph;
    }


    public List<MMPoint> getAllTowerPoints(List<MMEdge> trajectory) {
        List<MMPoint> points = new ArrayList<>();
        if (trajectory.size() == 0) return points;
        if (trajectory.size() == 1) {
            MMEdge edge = trajectory.get(0);
            points.add(edge.basePoint);
            points.add(edge.adjPoint);
            return points;
        }
        for (int i = 0; i < trajectory.size() - 1; ++i) {
            MMEdge cur = trajectory.get(i);
            if (cur.adjPoint == trajectory.get(i + 1).adjPoint || cur.adjPoint == trajectory.get(i + 1).basePoint) {
                points.add(cur.basePoint);
            } else {
                points.add(cur.adjPoint);
            }
        }
        MMEdge lastEdge = trajectory.get(trajectory.size() - 1);
        if (lastEdge.basePoint == trajectory.get(trajectory.size() - 2).adjPoint
                || lastEdge.basePoint == trajectory.get(trajectory.size() - 2).basePoint) {
            points.add(lastEdge.basePoint);
            points.add(lastEdge.adjPoint);
        } else {
            points.add(lastEdge.adjPoint);
            points.add(lastEdge.basePoint);
        }
        return points;
    }

    public List<MMPoint> getAllPoints(List<MMEdge> trajectory) {
        List<MMPoint> points = new ArrayList<>();
        if (trajectory.size() == 0) return points;
        if (trajectory.size() == 1) {
            MMEdge edge = trajectory.get(0);
            points.add(edge.basePoint);
            for (MMPoint point : edge.getPillarPoints()) {
                points.add(point);
            }
            points.add(edge.adjPoint);
            return points;
        }
        for (int i = 0; i < trajectory.size() - 1; ++i) {
            MMEdge cur = trajectory.get(i);
            if (cur.adjPoint == trajectory.get(i + 1).adjPoint || cur.adjPoint == trajectory.get(i + 1).basePoint) {
                points.add(cur.basePoint);
                for (MMPoint point : cur.getPillarPoints()) {
                    points.add(point);
                }
            } else {
                points.add(cur.adjPoint);
                for (int j = cur.getPillarPoints().size() - 1; j >= 0; --j) {
                    points.add(cur.getPillarPoints().get(j));
                }
            }
        }
        MMEdge lastEdge = trajectory.get(trajectory.size() - 1);
        if (lastEdge.basePoint == trajectory.get(trajectory.size() - 2).adjPoint
                || lastEdge.basePoint == trajectory.get(trajectory.size() - 2).basePoint) {
            points.add(lastEdge.basePoint);
            for (MMPoint point : lastEdge.getPillarPoints()) {
                points.add(point);
            }
            points.add(lastEdge.adjPoint);
        } else {
            points.add(lastEdge.adjPoint);
            for (int j = lastEdge.getPillarPoints().size() - 1; j >= 0; --j) {
                points.add(lastEdge.getPillarPoints().get(j));
            }
            points.add(lastEdge.basePoint);
        }
        return points;
    }

    /**
     * find mapping edges for for a list of points of type GPXEntry.
     *
     * @param pointList the points( on trajectory) of type TorVertex to be mapped
     * @param pathPoints a list of mapped points ( on virtual graph)
     * @param pathEdges a list of mapped edges.
     * @return the length of whole path
     *         metric: meter
     */
    public double fastMatch(List<GPXEntry> pointList, List<MMPoint> pathPoints, List<MMEdge> pathEdges) {
        if (MapMatching2 == null) throw new NullPointerException("invoke readPBF() first");

        if (pathPoints != null)
            pathPoints.clear();
        if (pathEdges != null)
            pathEdges.clear();

        List<MMPoint> points = new ArrayList<>();
        for (GPXEntry inputGPXEntry : pointList) {
            points.add(new MMPoint(inputGPXEntry));
        }
        return MapMatching2.runMMPoint(points, pathPoints, pathEdges);
    }

    /**
     * find mapping edges for for a list of points of type MMPoints.
     *
     * @param points the points( on trajectory) of type TorVertex to be mapped
     * @param pathPoints a list of mapped points ( on virtual graph)
     * @param pathEdges a list of mapped edges.
     * @return the length of whole path
     *         metric: meter
     */
    public double fastMatchMMPoint(List<MMPoint> points, List<MMPoint> pathPoints, List<MMEdge> pathEdges) {
        if (MapMatching2 == null) throw new NullPointerException("invoke readPBF() first");
        if (pathPoints != null)
            pathPoints.clear();
        if (pathEdges != null)
            pathEdges.clear();
        return MapMatching2.runMMPoint(points, pathPoints, pathEdges);
    }

    /**
     * using GraphHopper mapmatching api to find matched edges for a given trajectory
     *
     * @param inputGPXEntries a list of points of type GPXEntry representing the trajectory going to be matched.
     * @return a list of instances of type TorSegment representing segments mapping to the trajectory
     */
    public List<MMEdge> match(List<GPXEntry> inputGPXEntries) {
        if (mapMatching == null)
            throw new NullPointerException("invoke GraphHopperReadPDF() first");

        //GraphHopper API for doing the match
        MatchResult mr = mapMatching.doWork(inputGPXEntries);
        List<EdgeMatch> matches = mr.getEdgeMatches();
        List<MMEdge> segments = new ArrayList<>();
        for (EdgeMatch match : matches) {
            segments.add(new MMEdge(match, hopper));
        }
        return segments;
    }

    /**
     *
     * Given a list of points, the subroutine is to find the matching edges from virtual graph.
     * All edges(road on graph) are cached into outputEdgeList, and points( on edge) are stored in outputEdgeList
     *
     * Usage:
     * List<TorVertex> pathPoints = new ArrayList<>();
     * List<TorSegment> edges = new ArrayList<>();
     * trajectoryMapping.match(points, pathPoints, edges);
     *
     * @param pointList a list of points representing a trajectory, which is going to be mapped
     * @param outputPointList a list of points(unique) on edges
     * @param outputEdgeList a list of edges
     */
    public void match(List<MMPoint> pointList, List<MMPoint> outputPointList, List<MMEdge> outputEdgeList) {
        if (mapMatching == null)
            throw new NullPointerException("invoke GraphHopperReadPDF() first");

        List<GPXEntry> inputGPXEntries = new ArrayList<>(pointList.size());
        MatchResult mr = mapMatching.doWork(inputGPXEntries);
        List<EdgeMatch> matches = mr.getEdgeMatches();

        if (outputEdgeList != null && matches != null && matches.size() > 0) {
            for (EdgeMatch match : matches) {
                outputEdgeList.add(new MMEdge(match, hopper));
            }
        }
        if (outputPointList != null && matches != null && matches.size() > 0) {
            PointList prePointList = matches.get(0).getEdgeState().fetchWayGeometry(3);
            if (matches.size() == 1) {
                outputPointList.add(new MMPoint(prePointList.getLat(0), prePointList.getLon(0)));
                outputPointList.add(new MMPoint(prePointList.getLat(prePointList.size() - 1), prePointList.getLon(prePointList.size() - 1)));
                return;
            } else {
                outputPointList.add(new MMPoint(prePointList.getLat(0), prePointList.getLon(0)));
                if (!addPoints(outputPointList, matches)) {
                    outputPointList.clear();
                    outputPointList.add(new MMPoint(prePointList.getLat(prePointList.size() - 1), prePointList.getLon(prePointList.size() - 1)));
                    if (!addPoints(outputPointList, matches)) {
                        logger.error("impossible");
                    }
                }
            }
            for (int i = 1; i < outputPointList.size(); ++i) {
                if (outputPointList.get(i).equals(outputPointList.get(i - 1))) {
                    outputPointList.remove(i--);
                }
            }
        }
    }

    private boolean addPoints(List<MMPoint> outputPointList, List<EdgeMatch> matches) {
        int idx = 1;
        while (idx < matches.size()) {
            PointList curPointList = matches.get(idx).getEdgeState().fetchWayGeometry(3);
            if (curPointList.getLat(0) == outputPointList.get(outputPointList.size() - 1).getLat() && curPointList.getLon(0) == outputPointList.get(outputPointList.size() - 1).getLon()) {
                outputPointList.add(new MMPoint(curPointList.getLat(curPointList.size() - 1), curPointList.getLon(curPointList.size() - 1)));
            } else if (curPointList.getLat(curPointList.size() - 1) == outputPointList.get(outputPointList.size() - 1).getLat() && curPointList.getLon(curPointList.size() - 1) == outputPointList.get(outputPointList.size() - 1).getLon()) {
                outputPointList.add(new MMPoint(curPointList.getLat(0), curPointList.getLon(0)));
            } else {
                break;
            }
            ++idx;
        }
        return idx == matches.size();
    }

    public List<String> readTrajectory(String filePath, int limit) {
        File file = new File(filePath);
        List<String> result = new LinkedList<>();
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                int lineNum = 0;

                BlockingQueue<String> rawLineData = new ArrayBlockingQueue<String>(BLOCKING_QUEUE_SIZE);
                ExecutorService threadPool = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
                for (int i = 0; i < THREAD_NUM; ++i)
                    threadPool.execute(AppEngine.APPLICATION_CONTEXT.getBean(PortoTrajectoryFormatTask.class, rawLineData, String.valueOf(i), result));

                while ((line = reader.readLine()) != null) {
                    if (lineNum++ >= limit)
                        break;
                    rawLineData.put(line);
                    if (lineNum % 1000 == 0)
                        logger.info("lineNum={}, trajectory size={}, rawLineData={}", lineNum, result.size(), rawLineData.size());
                }

                for (int i = 0; i < THREAD_NUM; ++i)
                    rawLineData.put(Common.instance.STOP_CHARACTOR);

                threadPool.shutdown();
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
        return result;
    }

    /**
     * @param filePath
     * @param limit
     * @return
     */
    public List<List<GPXEntry>> readTrajectory(String filePath, int limit, LineFormatter formatter) {
        File file = new File(filePath);
        List<List<GPXEntry>> result = new LinkedList<>();
        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                int lineNum = 0;

                BlockingQueue<String> rawLineData = new ArrayBlockingQueue<String>(BLOCKING_QUEUE_SIZE);
                ExecutorService threadPool = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
                for (int i = 0; i < THREAD_NUM; ++i)
                    threadPool.execute(AppEngine.APPLICATION_CONTEXT.getBean(TrajectoryConvertTask.class, rawLineData, String.valueOf(i), result, formatter));

                while ((line = reader.readLine()) != null) {
                    if (lineNum >= limit)
                        break;
                    ++lineNum;
                    rawLineData.put(line);
                    if (lineNum % 1000 == 0)
                        logger.info("lineNum={}, trajectory size={}, rawLineData={}", lineNum, result.size(), rawLineData.size());
                    //if (rawLineData.size() > BLOCKING_QUEUE_SIZE / 2)
                    //Thread.sleep(100);
                }

                for (int i = 0; i < THREAD_NUM; ++i)
                    rawLineData.put(Common.instance.STOP_CHARACTOR);

                threadPool.shutdown();
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
        return result;
    }
}
