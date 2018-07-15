package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.model.*;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grid dataStructure for LEVI
 * It is for indexing all the points on virtual graph into the grid.
 *
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
@Component
public class NodeGridIndex {

    private static Logger logger = LoggerFactory.getLogger(NodeGridIndex.class);

    private final String INDEX_FILE_POINT = "dataStructure/ComplexRoadGridIndex.Point.idx";

    private final String INDEX_FILE_GRID_ID = "dataStructure/ComplexRoadGridIndex.hash.idx";

    private final static String SEPRATOR = ";";

    private float minLat = Float.MAX_VALUE, minLon = Float.MAX_VALUE, maxLat = -Float.MAX_VALUE, maxLon = -Float.MAX_VALUE, deltaLat, deltaLon, epsilon;

    private int horizontalTileNumber, verticalTileNumber;

    private final NodeInvertedIndex nodeInvertedIndex;

    /**
     * key for grid hash, value for point hash list
     */
    private Map<Integer, List<Integer>> grid;

    @Autowired
    public NodeGridIndex(NodeInvertedIndex nodeInvertedIndex) {
        this.nodeInvertedIndex = nodeInvertedIndex;
    }

    public void compress(Map<Integer, MMPoint> allPointMap){
        logger.info("Enter compress");
        byte[] SEPBYTE = new byte[]{0x1F, 0x3F, 0x5F, 0x7F};
        ByteBuffer buffer = ByteBuffer.allocate(4);
        //docs and freqs
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        ByteIntegerCODEC bic = new VariableByte();
        int idSeq = 0;
        Map<Integer, Integer> idReorderMap = new HashMap<>();
        for (MMPoint point : allPointMap.values()) {
            if (!idReorderMap.containsKey(point.getId())) {
                idReorderMap.put(point.getId(), idSeq++);
            }
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(INDEX_FILE_POINT));
             OutputStream posBufWriter = new FileOutputStream(INDEX_FILE_POINT + ".compress", false)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] idArray = line.split(";");
                int[] data = new int[idArray.length];
                for (int i = 0; i < idArray.length; ++i)
                    data[i] = Integer.parseInt(idArray[i]);
                for (int i = 0; i < data.length; i++) {
                    data[i] = idReorderMap.get(data[i]);
                }
                byte[] outArray = new byte[1000000];
                IntWrapper inPos = new IntWrapper(), outPos = new IntWrapper();
                bic.compress(data, inPos, data.length, outArray, outPos);
                posBufWriter.write(outArray, 0, outPos.get());
                posBufWriter.write(SEPBYTE);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit compress");
    }

    /**
     * Attention: crossing 0 is not supported such as (-50,100) (50,-100), the answer may be incorrect
     *
     * @param allPointMap key for TorVertex hash, value for instance of TorVertex itself.
     *                    note that the points is tower points, which means not only it have virtual points on trajectory,
     *                    but all points on virtual graph).
     * @param _epsilon  the granularity of a tile (meter)
     */
    public void buildIndex(Map<Integer, MMPoint> allPointMap, float _epsilon) {
        logger.info("Enter buildIndex");
        this.epsilon = _epsilon;
        //find bounding
        minLat = Float.MAX_VALUE;
        minLon = Float.MAX_VALUE;
        maxLat = -Float.MAX_VALUE;
        maxLon = -Float.MAX_VALUE;
        Collection<MMPoint> allPoints = allPointMap.values();
        for (MMPoint point : allPoints) {
            if (point.getLat() < minLat) minLat = (float) point.getLat();
            if (point.getLat() > maxLat) maxLat = (float) point.getLat();
            if (point.getLon() < minLon) minLon = (float) point.getLon();
            if (point.getLon() > maxLon) maxLon = (float) point.getLon();
        }

        //create grid
        double horizontal_span = GeoUtil.distance(maxLat, maxLat, minLon, maxLon);  //horizontal width of the grid
        double vertical_span = GeoUtil.distance(maxLat, minLat, minLon, minLon);    //vertical width of the grid

        this.horizontalTileNumber = (int) (horizontal_span / epsilon);
        this.verticalTileNumber = (int) (vertical_span / epsilon);
        this.deltaLat = (maxLat - minLat) / this.verticalTileNumber;
        this.deltaLon = (maxLon - minLon) / this.horizontalTileNumber;

        logger.info("start to insert points, (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}, point size: {}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, allPoints.size());
        //key for grid hash, value for point hash list
        Map<Integer, Set<Integer>> grid = new HashMap<>(this.horizontalTileNumber * this.verticalTileNumber + 1);
        //insert points
        ExecutorService threadPool = new ThreadPoolExecutor(0, 15, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger controller = new AtomicInteger(100000);

        for (MMPoint point : allPoints) {
            while (controller.intValue() == 0) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            threadPool.execute(new InsertPointTask(allPointMap, point, counter, controller, grid));
        }
        logger.info("start to shutdown");
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }

        logger.info("store the dataStructure into the disk");
        File file = new File(INDEX_FILE_POINT);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter idWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_GRID_ID, false), StandardCharsets.UTF_8)));
             BufferedWriter pointWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_POINT, false), StandardCharsets.UTF_8)))) {
            //first write some arguments
            idWriter.write(this.minLat + Common.instance.SEPARATOR2);
            idWriter.write(this.minLon + Common.instance.SEPARATOR2);
            idWriter.write(this.maxLat + Common.instance.SEPARATOR2);
            idWriter.write(this.maxLon + Common.instance.SEPARATOR2);
            idWriter.write(this.deltaLat + Common.instance.SEPARATOR2);
            idWriter.write(this.deltaLon + Common.instance.SEPARATOR2);
            idWriter.write(this.horizontalTileNumber + Common.instance.SEPARATOR2);
            idWriter.write(this.verticalTileNumber + Common.instance.SEPARATOR2);
            idWriter.write(this.epsilon + Common.instance.SEPARATOR2);
            idWriter.newLine();

            PriorityQueue<Integer> gridPriorityQueue = new PriorityQueue<>(Comparator.naturalOrder());
            for (Map.Entry<Integer, Set<Integer>> gridEntry : grid.entrySet()) {
                Set<Integer> pointIDSet = gridEntry.getValue();
                if (pointIDSet != null) {
                    //write grid hash
                    idWriter.write(gridEntry.getKey() + "");
                    idWriter.newLine();
                    //write point hash list
                    boolean firstLinePoint = true;
                    gridPriorityQueue.addAll(pointIDSet);
                    while (!gridPriorityQueue.isEmpty()) {
                        int pointID = gridPriorityQueue.poll();
                        if (firstLinePoint)
                            firstLinePoint = false;
                        else
                            pointWriter.write(SEPRATOR);
                        pointWriter.write(pointID + "");
                    }
                    pointWriter.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        load();
        logger.info("Exit buildIndex");
    }

    /**
     * once dataStructure is built, it will be stored in the disk
     * return true if dataStructure can be loaded from disk
     * return false otherwise
     *
     * @return
     */
    public boolean load() {
        logger.info("Enter load");
        File file = new File(INDEX_FILE_POINT);
        String line = null, pointLine = null;
        if (this.grid == null && file.exists()) {
            try (BufferedReader idReader = new BufferedReader(new FileReader(INDEX_FILE_GRID_ID));
                 BufferedReader pointReader = new BufferedReader(new FileReader(INDEX_FILE_POINT))) {
                line = idReader.readLine();
                String[] trajLineArray = line.split(Common.instance.SEPARATOR2);
                this.minLat = Float.parseFloat(trajLineArray[0]);
                this.minLon = Float.parseFloat(trajLineArray[1]);
                this.maxLat = Float.parseFloat(trajLineArray[2]);
                this.maxLon = Float.parseFloat(trajLineArray[3]);
                this.deltaLat = Float.parseFloat(trajLineArray[4]);
                this.deltaLon = Float.parseFloat(trajLineArray[5]);
                this.horizontalTileNumber = Integer.parseInt(trajLineArray[6]);
                this.verticalTileNumber = Integer.parseInt(trajLineArray[7]);
                this.epsilon = Float.parseFloat(trajLineArray[8]);
                logger.info("(minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}, epsilon: {}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, epsilon);
                this.grid = new HashMap<>();
                int process = 0;
                while ((line = idReader.readLine()) != null) {
                    if (process++ % 10000 == 0)
                        logger.info("counter: {}", process);
                    int gridID = Integer.parseInt(line);
                    pointLine = pointReader.readLine();
                    String[] pointLineArray = pointLine.split(SEPRATOR);
                    List<Integer> pointIDList = new ArrayList<>(pointLineArray.length);
                    this.grid.put(gridID, pointIDList);
                    for (int i = 0; i < pointLineArray.length; i++) {
                        pointIDList.add(Integer.parseInt(pointLineArray[i]));
                    }
                }
                logger.info("grid size: {}", this.grid.size());
                logger.info("start to load node dataStructure");
                nodeInvertedIndex.load();
            } catch (Exception e) {
                e.printStackTrace();
            }

            logger.info("Exit load");
            return true;
        }
        logger.info("Exit load");
        return false;
    }

    private double getLat(Map<Integer, MMPoint> allPointMap, ComplexGridIndexPoint point) {
        return allPointMap.get(point.getPointID()).getLat();
    }

    private double getLon(Map<Integer, MMPoint> allPointMap, ComplexGridIndexPoint point) {
        return allPointMap.get(point.getPointID()).getLon();
    }

    private int calculateGridID(double lat, double lon) {
        int row = (int) ((this.maxLat - lat) / this.deltaLat);
        int col = (int) ((lon - this.minLon) / this.deltaLon);
        int gridID = row * this.horizontalTileNumber + col;
        if (gridID < 0) return 0;
        if (gridID > this.horizontalTileNumber * this.verticalTileNumber) return this.horizontalTileNumber * this.verticalTileNumber - 1;
        return gridID;
    }

    /**
     * key for trajectory hash, value is the corresponding position
     *
     * @param point
     * @return
     */
    private List<CandidatePoint> find(Point point) {
        int pos = calculateGridID(point.getLat(), point.getLon());
        return find(pos);
    }

    private List<CandidatePoint> find(int pos) {
        List<Integer> pointIDList = this.grid.get(pos);
        List<CandidatePoint> candidate = new LinkedList<>();
        if (pointIDList != null) {
            for (Integer pointID : pointIDList) {
                List<? extends au.edu.rmit.trajectory.similarity.model.Pair> res = nodeInvertedIndex.find(pointID);
                if (res != null) {//not every point is traversed by a trajectory
                    for (au.edu.rmit.trajectory.similarity.model.Pair re : res) {
                        candidate.add(new CandidatePoint(pointID, (int) re.getKey(), (int) re.getValue()));
                    }
                }
            }
        }
        return candidate;
    }

    /**
     * find all hash of points within the tile in grid,
     * @param lat latitude indicates the location of the tile
     * @param lon longitude
     * @see NodeInvertedIndex#find(int)
     * @return a list of hash of points within the tile in grid
     */
    public List<Integer> find(double lat, double lon) {
        int pos = calculateGridID(lat, lon);
        return this.grid.get(pos);
    }

    public Collection<Integer> findRange(double minLat, double minLon, double maxLat, double maxLon) {
        int leftUpperID = calculateGridID(maxLat, minLon);
        int rightUpperID = calculateGridID(maxLat, maxLon);
        int leftLowerID = calculateGridID(minLat, minLon);
        int rightLowerID = calculateGridID(minLat, maxLon);
        Set<Integer> result = new HashSet<>();
        for (int i = leftUpperID; i <= rightUpperID; ++i) {
            int id = i;
            int dif = (leftLowerID - leftUpperID) / this.horizontalTileNumber;
            while (dif-- > 0) {
                List<Integer> list = this.grid.get(id);
                if (list != null)
                    result.addAll(this.grid.get(id));
                id += this.horizontalTileNumber;
            }
        }
        return result;
    }

    private void insert(double lat, double lon, int pointID, Map<Integer, Set<Integer>> grid) {
        synchronized (NodeGridIndex.class) {
            if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) return;
            int tileId = calculateGridID(lat, lon);
            Set<Integer> pointIDSet = grid.computeIfAbsent(tileId, k -> new HashSet<>());
            pointIDSet.add(pointID);
        }
    }

    /**
     * for one point, 9 points should be insert
     *
     * @param point
     */
    public void insert(Map<Integer, MMPoint> allPointMap, MMPoint point, Map<Integer, Set<Integer>> grid) {
        double lowerLat = GeoUtil.increaseLatitude(point.getLat(), -epsilon);
        double upperLat = GeoUtil.increaseLatitude(point.getLat(), epsilon);
        double lowerLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -epsilon);
        double upperLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), epsilon);

        //here point position represents its hash
        insert(upperLat, lowerLon, point.getId(), grid);
        insert(upperLat, point.getLon(), point.getId(), grid);
        insert(upperLat, upperLon, point.getId(), grid);

        insert(point.getLat(), lowerLon, point.getId(), grid);
        insert(point.getLat(), point.getLon(), point.getId(), grid);
        insert(point.getLat(), upperLon, point.getId(), grid);

        insert(lowerLat, lowerLon, point.getId(), grid);
        insert(lowerLat, point.getLon(), point.getId(), grid);
        insert(lowerLat, upperLon, point.getId(), grid);
    }

    public void delete() {
        try {
            Files.deleteIfExists(Paths.get(INDEX_FILE_GRID_ID));
            Files.deleteIfExists(Paths.get(INDEX_FILE_POINT));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void incrementallyFind(int leftUpperPos, int rightUpperPos, int leftLowerPos, int rightLowerPos, Set<CandidatePoint> candidateList) {

        for (int i = leftUpperPos; i < rightUpperPos; ++i) {
            List<CandidatePoint> idList = find(i);
            if (idList != null)
                candidateList.addAll(idList);
        }
        for (int i = rightUpperPos; i < rightLowerPos; i += this.horizontalTileNumber) {
            List<CandidatePoint> idList = find(i);
            if (idList != null)
                candidateList.addAll(idList);
        }
        for (int i = rightLowerPos; i < leftLowerPos; --i) {
            List<CandidatePoint> idList = find(i);
            if (idList != null)
                candidateList.addAll(idList);
        }
        for (int i = leftLowerPos; i < leftUpperPos; i -= this.horizontalTileNumber) {
            List<CandidatePoint> idList = find(i);
            if (idList != null)
                candidateList.addAll(idList);
        }
    }

    private int findLeftUpperPos(int pos) {
        if (pos % this.horizontalTileNumber == 0) {
            if (pos < this.horizontalTileNumber) return pos;
            int ans = pos - this.horizontalTileNumber;
            return ans;
        } else {
            if (pos < this.horizontalTileNumber) return pos - 1;
            int ans = pos - this.horizontalTileNumber - 1;
            return ans;
        }
    }

    private int findRightUpperPos(int pos) {
        if (pos % (this.horizontalTileNumber - 1) == 0) {
            if (pos < this.horizontalTileNumber) return pos;
            int ans = pos - this.horizontalTileNumber;
            return ans;
        } else {
            if (pos < this.horizontalTileNumber) return pos + 1;
            int ans = pos - this.horizontalTileNumber + 1;
            return ans;
        }
    }

    private int findLeftLowerPos(int pos) {
        if (pos % this.horizontalTileNumber == 0) {
            if (pos >= this.horizontalTileNumber * (this.verticalTileNumber - 1)) return pos;
            int ans = pos + this.horizontalTileNumber;
            return ans;
        } else {
            if (pos >= this.horizontalTileNumber * (this.verticalTileNumber - 1)) return pos - 1;
            int ans = pos + this.horizontalTileNumber - 1;
            return ans;
        }
    }

    private int findRightLowerPos(int pos) {
        if (pos % (this.horizontalTileNumber - 1) == 0) {
            if (pos >= this.horizontalTileNumber * (this.verticalTileNumber - 1)) return pos;
            int ans = pos + this.horizontalTileNumber;
            return ans;
        } else {
            if (pos >= this.horizontalTileNumber * (this.verticalTileNumber - 1)) return pos + 1;
            int ans = pos + this.horizontalTileNumber + 1;
            return ans;
        }
    }

    private double find(Point point, int step, MeasureType measureType, Map<Integer, Double> existingTrajIDLowerBound, Map<Integer, MMPoint> allPointMap) {
        double maxDistance = 0;
        //find the nearest pair between a trajectory and query point
        Map<Integer, Double> nearestDistance = new HashMap<>();
        //trajectory hash, point hash list
        Set<CandidatePoint> list = new HashSet<>();
        if (step == 0) {
            list.addAll(find(point));
        } else {
            int pos = calculateGridID(point.getLat(), point.getLon());
            int leftUpperPos = findLeftUpperPos(pos);
            int rightUpperPos = findRightUpperPos(pos);
            int leftLowerPos = findLeftLowerPos(pos);
            int rightLowerPos = findRightLowerPos(pos);
            while (--step > 0) {
                leftUpperPos = findLeftUpperPos(leftUpperPos);
                rightUpperPos = findRightUpperPos(rightUpperPos);
                leftLowerPos = findLeftLowerPos(leftLowerPos);
                rightLowerPos = findRightLowerPos(rightLowerPos);
            }
            incrementallyFind(leftUpperPos, rightUpperPos, leftLowerPos, rightLowerPos, list);
        }
        Map<Integer, Double> cacheDistance = new HashMap<>();
        if (list != null) {
            //pair is a point in the form of (trajectory hash, position)
            for (CandidatePoint candidatePoint : list) {
                Double distance = cacheDistance.get(candidatePoint.pointID);
                if (distance == null) {
                    distance = GeoUtil.distance(point, allPointMap.get(candidatePoint.pointID));
                    cacheDistance.put(candidatePoint.pointID, distance);
                }
                if (distance > maxDistance) maxDistance = distance;
                Double lowerBound = nearestDistance.get(candidatePoint.trajectoryID);
                if (lowerBound == null) {
                    nearestDistance.put(candidatePoint.trajectoryID, distance);
                } else {
                    if (measureType == MeasureType.DTW)
                        nearestDistance.put(candidatePoint.trajectoryID, lowerBound + distance);
                    else
                        nearestDistance.put(candidatePoint.trajectoryID, Math.min(distance, lowerBound));
                }
            }
        }
        //update lower bound for each trajectory
        for (Map.Entry<Integer, Double> entry : nearestDistance.entrySet()) {
            Double lowerBound = existingTrajIDLowerBound.get(entry.getKey());
            if (lowerBound == null) {
                existingTrajIDLowerBound.put(entry.getKey(), entry.getValue());
            } else {
                existingTrajIDLowerBound.put(entry.getKey(), Math.max(entry.getValue(), lowerBound));
            }
        }
        return maxDistance;
    }

    /**
     * @param trajectoryMap
     * @param allPointMap
     * @param trajectory
     * @param k
     * @param measureType
     * @param candidateNumberList
     * @param scannedCandidateNumberList
     * @return
     */
    public List<Integer> findTopK(Map<Integer, Trajectory> trajectoryMap, Map<Integer, MMPoint> allPointMap, Trajectory trajectory, int k, MeasureType measureType, List<Integer> candidateNumberList, List<Integer> scannedCandidateNumberList, List<Long> lookupTimeList) {
        if (this.grid == null)
            throw new IllegalStateException("invoke buildTorGraph first");
        List<MMPoint> queryPoints = trajectory.getMMPoints();
        double bestSoFar = Double.MAX_VALUE, unseenLowerBound = 0;
        int step = 0;
        SimilarityMeasure<MMPoint> similarityMeasure = Common.instance.SIM_MEASURE;
        //trajectory hash and score
        PriorityQueue<Pair> topKHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p2.score, p1.score));
        int candidateNumber = 0;
        Set<Integer> visitTrajectorySet = new HashSet<>();
        int scannedCandidateNumber = 0;
        long startTime, endTime, lookupTime = 0;
        while (true) {
            startTime = System.nanoTime();
            if (bestSoFar < unseenLowerBound && topKHeap.size() >= k) break;
            unseenLowerBound = 0;
            //each query point match with the nearest point of a trajectory,
            // and the lower bound is the maximun distance between a query and existing points of a trajectory
            Map<Integer, Double> trajIDLowerBound = new HashMap<>();
            //find candiates incrementally and calculate their lowerbound
            for (MMPoint queryPoint : queryPoints) {
                double maxDis = find(queryPoint, step, measureType, trajIDLowerBound, allPointMap);
                switch (measureType) {
                    case DTW:
                        unseenLowerBound += maxDis;
                        break;
                    case Hausdorff:
                        if (unseenLowerBound < maxDis) unseenLowerBound = maxDis;
                        break;
                    case Frechet:
                        if (unseenLowerBound < maxDis) unseenLowerBound = maxDis;
                        break;
                }
            }
            //rank trajectories by their lower bound
            List<Map.Entry<Integer, Double>> tempList = new ArrayList<>(trajIDLowerBound.entrySet());
            tempList.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey()));
            PriorityQueue<Map.Entry<Integer, Double>> rankedTrajectories = new PriorityQueue<>(Map.Entry.comparingByValue());
            for (Map.Entry<Integer, Double> entry : tempList) {
                if (!visitTrajectorySet.contains(entry.getKey()))
                    rankedTrajectories.add(entry);
            }
            //mark visited trajectories
            visitTrajectorySet.addAll(trajIDLowerBound.keySet());
            candidateNumber = trajIDLowerBound.size();
            endTime = System.nanoTime();
            lookupTime += (endTime - startTime);
            //calculate exact distance for each candidate
            while (rankedTrajectories.size() > 0) {
                Map.Entry<Integer, Double> entry = rankedTrajectories.poll();
                int trajID = entry.getKey();
                double score = 0;
                switch (measureType) {
                    case DTW:
                        score = similarityMeasure.fastDynamicTimeWarping(trajectoryMap.get(trajID).getMMPoints(), queryPoints, 10, bestSoFar, null);
                        break;
                    case Hausdorff:
                        score = similarityMeasure.Hausdorff(trajectoryMap.get(trajID).getMMPoints(), queryPoints);
                        break;
                    case Frechet:
                        score = similarityMeasure.Frechet(trajectoryMap.get(trajID).getMMPoints(), queryPoints);
                        break;
                }
                ++scannedCandidateNumber;
                Pair pair = new Pair(trajID, score);
                topKHeap.add(pair);
                if (topKHeap.size() > k) {
                    topKHeap.poll();
                    bestSoFar = topKHeap.peek().score;
                    if (rankedTrajectories.size() == 0) break;
                    if (bestSoFar < rankedTrajectories.peek().getValue())
                        break;
                }
            }
            bestSoFar = topKHeap.peek().score;
            ++step;
            if (step == 3) {
                logger.error("step = 3");
                break;
            }
        }
        List<Integer> resIDList = new ArrayList<>();
        while (topKHeap.size() > 0) {
            resIDList.add(topKHeap.poll().trajectoryID);
        }
        if (lookupTimeList != null) {
            lookupTimeList.add(lookupTime / 1000000L);
        }
        if (candidateNumberList != null) {
            candidateNumberList.add(visitTrajectorySet.size());
//            logger.info("candidate number: {}, scannedCandidateNumber = {}, step = {}", visitTrajectorySet.size(), scannedCandidateNumber, step);
        }
        if (scannedCandidateNumberList != null) {
            scannedCandidateNumberList.add(scannedCandidateNumber);
        }
        return resIDList;
    }

    class InsertPointTask implements Runnable {

        final MMPoint point;

        final AtomicInteger process;

        final AtomicInteger controller;

        final Map<Integer, Set<Integer>> grid;

        final Map<Integer, MMPoint> allPointMap;

        InsertPointTask(Map<Integer, MMPoint> allPointMap, MMPoint point, AtomicInteger process, AtomicInteger controller, Map<Integer, Set<Integer>> grid) {
            this.point = point;
            this.process = process;
            this.controller = controller;
            this.grid = grid;
            this.allPointMap = allPointMap;
        }

        @Override
        public void run() {
            controller.decrementAndGet();
            process.incrementAndGet();
            if (process.intValue() % 100000 == 0)
                logger.info("counter: {}, grid size: {}, queue size: {}", process.intValue(), grid.size(), controller.intValue());
            insert(allPointMap, point, grid);
            controller.incrementAndGet();
        }
    }

    class Pair {
        public final int trajectoryID;
        public final double score;

        Pair(int trajectoryID, double score) {
            this.trajectoryID = trajectoryID;
            this.score = score;
        }
    }

    class CandidatePoint {
        public final int pointID;
        public final int trajectoryID;
        public final int position;

        CandidatePoint(int pointID, int trajectoryID, int position) {
            this.pointID = pointID;
            this.trajectoryID = trajectoryID;
            this.position = position;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CandidatePoint that = (CandidatePoint) o;
            return pointID == that.pointID &&
                    trajectoryID == that.trajectoryID &&
                    position == that.position;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pointID, trajectoryID, position);
        }
    }
}
