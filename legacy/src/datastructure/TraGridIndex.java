package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.algorithm.SimilarityMeasure;
import au.edu.rmit.trajectory.similarity.model.GridIndexPoint;
import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Point;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grid dataStructure could be used in DTW
 * It is for indexing all the ids of trajectory into the grid.
 * It is possible that same traId appears in different tiles which is the case of trajectory across tiles.
 *
 * @author forrest0402
 */
@Component
public class TraGridIndex {

    private static Logger logger = LoggerFactory.getLogger(TraGridIndex.class);

    private final String INDEX_FILE = "dataStructure/RoadGridIndex.idx";

    private float minLat = Float.MAX_VALUE, minLon = Float.MAX_VALUE, maxLat = -Float.MAX_VALUE, maxLon = -Float.MAX_VALUE,
                  deltaLat, deltaLon, epsilon;

    private int horizontalTileNumber, verticalTileNumber;

    /**
     * key for tile hash, value for ( a list of hash of trajectory)
     * note a trajectory may across more than one tiles.
     */
    private Map<Integer, List<Integer>> grid;

    /**
     * Attention: crossing 0 is not supported such as (-50,100) (50,-100), the answer may be incorrect
     *
     * @param allPointsOnTra a list of instances of type GridIndexPoint.
     *                  note that the points contained by more than one trajectories should be appear more than one time.
     * @param _epsilon  the granularity of a tile (meters)
     */
    public void buildIndex(List<GridIndexPoint> allPointsOnTra, float _epsilon) {
        logger.info("Enter buildIndex - point size: {}", allPointsOnTra.size());
        this.epsilon = _epsilon;

        //find bounding box
        minLat = Float.MAX_VALUE;
        minLon = Float.MAX_VALUE;
        maxLat = -Float.MAX_VALUE;
        maxLon = -Float.MAX_VALUE;
        for (GridIndexPoint point : allPointsOnTra) {
            if (point.getLat() < minLat) minLat = (float) point.getLat();
            if (point.getLat() > maxLat) maxLat = (float) point.getLat();
            if (point.getLon() < minLon) minLon = (float) point.getLon();
            if (point.getLon() > maxLon) maxLon = (float) point.getLon();
        }

        //create grid
        //latitude: measure vertical
        //longitude: measure horizontal
        double horizontal_span = GeoUtil.distance(maxLat, maxLat, maxLon, minLon);  //width of the grid
        double vertical_span = GeoUtil.distance(maxLat, minLat, maxLon, maxLon);    //height of the grid
        this.horizontalTileNumber = (int) (horizontal_span / epsilon);
        this.verticalTileNumber = (int) (vertical_span / epsilon);
        this.deltaLat = (maxLat - minLat) / this.verticalTileNumber;                //span of a tile vertically
        this.deltaLon = (maxLon - minLon) / this.horizontalTileNumber;              //span of a tile horizontally

        logger.info("start to insert points, (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}, point size: {}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, allPointsOnTra.size());
        Map<Integer, Set<Integer>> grid = new HashMap<>(this.horizontalTileNumber * this.verticalTileNumber + 1);

        //insert points
        ExecutorService threadPool = new ThreadPoolExecutor(0, 15, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger controller = new AtomicInteger(100000);
        Iterator<GridIndexPoint> iter = allPointsOnTra.iterator();
        while (iter.hasNext()) {
            GridIndexPoint point = iter.next();
            while (controller.intValue() == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //each time the task is executed, there will be 9 points get inserted.
            threadPool.execute(new InsertPointTask(point, counter, controller, grid));
            iter.remove();
        }
        logger.info("start to shutdown");
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("store the dataStructure into the disk");
        File file = new File(INDEX_FILE);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter bw = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE, false), StandardCharsets.UTF_8)))) {
            //first line contains parameters.
            bw.write(this.minLat + Common.instance.SEPARATOR2);
            bw.write(this.minLon + Common.instance.SEPARATOR2);
            bw.write(this.maxLat + Common.instance.SEPARATOR2);
            bw.write(this.maxLon + Common.instance.SEPARATOR2);
            bw.write(this.deltaLat + Common.instance.SEPARATOR2);
            bw.write(this.deltaLon + Common.instance.SEPARATOR2);
            bw.write(this.horizontalTileNumber + Common.instance.SEPARATOR2);
            bw.write(this.verticalTileNumber + Common.instance.SEPARATOR2);
            bw.write(this.epsilon + Common.instance.SEPARATOR2);
            bw.newLine();
            for (Map.Entry<Integer, Set<Integer>> gridEntry : grid.entrySet()) {
                Set<Integer> traIdSet = gridEntry.getValue();
                if (traIdSet != null) {
                    //write traIdSet dataStructure
                    bw.write(gridEntry.getKey() + "");
                    List<Integer> trajectoryList = new ArrayList<>(traIdSet);
                    trajectoryList.sort(Comparator.naturalOrder());
                    for (Integer id : trajectoryList) {
                        bw.write(Common.instance.SEPARATOR2);
                        bw.write(id + "");
                    }
                    bw.newLine();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit buildIndex");
    }

    /**
     * once dataStructure is built, it will be stored in the disk
     *
     * @return true if dataStructure can be loaded from disk
     *         false otherwise
     */
    public boolean load() {
        logger.info("Enter load");
        File file = new File(INDEX_FILE);
        if (file.exists()) {
            try (BufferedReader bw = new BufferedReader(new FileReader(INDEX_FILE))) {
                String line = bw.readLine();
                String[] lineArray = line.split(Common.instance.SEPARATOR2);
                this.minLat = Float.parseFloat(lineArray[0]);
                this.minLon = Float.parseFloat(lineArray[1]);
                this.maxLat = Float.parseFloat(lineArray[2]);
                this.maxLon = Float.parseFloat(lineArray[3]);
                this.deltaLat = Float.parseFloat(lineArray[4]);
                this.deltaLon = Float.parseFloat(lineArray[5]);
                this.horizontalTileNumber = Integer.parseInt(lineArray[6]);
                this.verticalTileNumber = Integer.parseInt(lineArray[7]);
                this.epsilon = Float.parseFloat(lineArray[8]);
                this.grid = new HashMap<>();
                int process = 0;
                while ((line = bw.readLine()) != null) {
                    if (++process % 10000 == 0) {
                        logger.info("counter: {}", process);
                    }
                    lineArray = line.split(Common.instance.SEPARATOR2);
                    int gridID = Integer.parseInt(lineArray[0]);
                    List<Integer> trajIDList = new ArrayList<>(lineArray.length);
                    for (int i = 1; i < lineArray.length; ++i) {
                        trajIDList.add(Integer.parseInt(lineArray[i]));
                    }
                    this.grid.put(gridID, trajIDList);
                }
                logger.info("grid size: {}", this.grid.size());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            logger.info("Exit load");
            return true;
        }
        logger.info("Exit load");
        return false;
    }

    private int calculateGridID(double lat, double lon) {
        int row = (int) ((this.maxLat - lat) / this.deltaLat);
        int col = (int) ((lon - this.minLon) / this.deltaLon);
        return row * this.horizontalTileNumber + col;
    }

    /**
     * key for trajectory hash, value is the corresponding position
     *
     * @param point
     * @return
     */
    public List<Integer> find(Point point) {
        int pos = calculateGridID(point.getLat(), point.getLon());
        if (!this.grid.containsKey(pos)) {
            //Attention, this is possible, because that grid doesn't contain any points
            logger.error("pos = {}, point=({},{}) (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}", pos, point.getLat(), point.getLon(), minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, this.grid.size());
        }
        return this.grid.get(pos);
    }

    private void incrementallyFind(int leftUpperPos, int rightUpperPos, int leftLowerPos, int rightLowerPos, Set<Integer> trajIDSet) {

        for (int i = leftUpperPos; i < rightUpperPos; ++i) {
            List<Integer> idList = this.grid.get(i);
            if (idList != null)
                trajIDSet.addAll(idList);
        }
        for (int i = rightUpperPos; i < rightLowerPos; i += this.horizontalTileNumber) {
            List<Integer> idList = this.grid.get(i);
            if (idList != null)
                trajIDSet.addAll(idList);
        }
        for (int i = rightLowerPos; i < leftLowerPos; --i) {
            List<Integer> idList = this.grid.get(i);
            if (idList != null)
                trajIDSet.addAll(idList);
        }
        for (int i = leftLowerPos; i < leftUpperPos; i -= this.horizontalTileNumber) {
            List<Integer> idList = this.grid.get(i);
            if (idList != null)
                trajIDSet.addAll(idList);
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

    private void find(Point point, int step, Set<Integer> trajIDSet) {
        if (step == 0) {
            List<Integer> list = find(point);
            if (list != null)
                trajIDSet.addAll(list);
        } else {
            int pos = calculateGridID(point.getLat(), point.getLon());
            int leftUpperPos = findLeftUpperPos(pos);
            int rightUpperPos = findRightUpperPos(pos);
            int leftLowerPos = findLeftLowerPos(pos);
            int rightLowerPos = findRightLowerPos(pos);
            while (--step > 0) {
                leftUpperPos = findLeftUpperPos(pos);
                rightUpperPos = findRightUpperPos(pos);
                leftLowerPos = findLeftLowerPos(pos);
                rightLowerPos = findRightLowerPos(pos);
            }
            incrementallyFind(leftUpperPos, rightUpperPos, leftLowerPos, rightLowerPos, trajIDSet);
        }
    }


    public List<Integer> findTopK(Map<Integer, Trajectory> trajectoryMap, Trajectory trajectory, int k, MeasureType measureType, List<Integer> candidateNumberList, List<Integer> scannedCandidateNumberList, List<Long> lookupTimeList) {
        if (this.grid == null)
            throw new IllegalStateException("invoke buildTorGraph first");
        List<MMPoint> queryPoints = trajectory.getMMPoints();
        double bestSoFar = Double.MAX_VALUE, unseenLowerBound = 0;
        int step = 0;
        SimilarityMeasure<MMPoint> similarityMmeasure = Common.instance.SIM_MEASURE;
        PriorityQueue<Pair> topKHeap = new PriorityQueue<>((p1, p2) -> Double.compare(p2.score, p1.score));
        int candidateNumber = 0;
        Set<Integer> visit = new HashSet<>();
        AtomicInteger scannedCandidateNumber = new AtomicInteger(0);
        long lookupTime = 0, startTime, endTime;
        while (true) {
            startTime = System.nanoTime();
            if (bestSoFar < unseenLowerBound && topKHeap.size() >= k) break;
            //find candiate incrementally
            Set<Integer> trajIDSet = new HashSet<>();
            for (MMPoint queryPoint : queryPoints) {
                find(queryPoint, step, trajIDSet);
            }
            Iterator<Integer> iterator = trajIDSet.iterator();
            while (iterator.hasNext()) {
                Integer next = iterator.next();
                if (visit.contains(next))
                    iterator.remove();
            }
            visit.addAll(trajIDSet);
            endTime = System.nanoTime();
            lookupTime += (endTime - startTime);
            //calculate exact distance
            for (Integer trajID : trajIDSet) {
                double score = 0;
                switch (measureType) {
                    case DTW:
                        score = similarityMeasure.fastDynamicTimeWarping(trajectoryMap.get(trajID).getMMPoints(), queryPoints, 10, bestSoFar, scannedCandidateNumber);
                        break;
                }
                Pair pair = new Pair(trajID, score);
                topKHeap.add(pair);
                if (topKHeap.size() > k) topKHeap.poll();
            }
            bestSoFar = topKHeap.peek().score;
            //calculate upperbound for unseen trajectories
            switch (measureType) {
                case DTW:
                    unseenLowerBound = queryPoints.size() * epsilon / 2 + queryPoints.size() * epsilon * step;
                    break;
            }
            ++step;
        }
        List<Integer> resIDList = new ArrayList<>();
        while (topKHeap.size() > 0) {
            resIDList.add(topKHeap.poll().trajectoryID);
        }
        if (candidateNumberList != null) {
            candidateNumber = visit.size();
            candidateNumberList.add(candidateNumber);
            logger.info("candidate number: {}, step = {}", candidateNumber, step);
        }
        if (lookupTimeList != null)
            lookupTimeList.add(lookupTime / 1000000L);
        if (scannedCandidateNumberList != null) {
            scannedCandidateNumberList.add(scannedCandidateNumber.intValue());
        }
        return resIDList;
    }

    /**
     * insert the point into tile that the point should resides in.
     * @param lat latitude of the point to be inserted
     * @param lon longitude of the point to be inserted
     * @param trajectoryID hash of the trajectory containing the point
     * @param grid in-memory grid dataStructure of type Map
     * @see #calculateGridID(double, double)
     */
    private void insert(double lat, double lon, int trajectoryID, Map<Integer, Set<Integer>> grid) {
        synchronized (ComplexTraGridIndex.class) {
            if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) return;
            int tileId = calculateGridID(lat, lon);

            Set<Integer> trajectoryIdSet = grid.computeIfAbsent(tileId, k -> new HashSet<>());
            trajectoryIdSet.add(trajectoryID);
        }
    }

    /**
     * For each point, 9 points should be insert.
     * It looks like this "田". where each joint point by two edges is a point to be inserted.
     * The origin point is located on middle.
     *
     * @param point the point to be inserted into grid.
     * @param grid in-memory grid dataStructure of type Map
     *
     * @see #insert(double, double, int, Map)
     */
    public void insert(GridIndexPoint point, Map<Integer, Set<Integer>> grid) {
        double lowerLat = GeoUtil.increaseLatitude(point.getLat(), -epsilon);
        double upperLat = GeoUtil.increaseLatitude(point.getLat(), epsilon);
        double lowerLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -epsilon);
        double upperLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), epsilon);
        double originLat = point.getLat();
        double originLng = point.getLon();

        insert(upperLat, lowerLon, point.getTrajectoryId(), grid);
        insert(upperLat, originLng, point.getTrajectoryId(), grid);
        insert(upperLat, upperLon, point.getTrajectoryId(), grid);

        insert(originLat, lowerLon, point.getTrajectoryId(), grid);
        insert(originLat, originLng, point.getTrajectoryId(), grid);
        insert(originLat, upperLon, point.getTrajectoryId(), grid);

        insert(lowerLat, lowerLon, point.getTrajectoryId(), grid);
        insert(lowerLat, originLng, point.getTrajectoryId(), grid);
        insert(lowerLat, upperLon, point.getTrajectoryId(), grid);
    }

    public void delete() {
        throw new UnsupportedOperationException("");
    }

    private class InsertPointTask implements Runnable {

        final GridIndexPoint point;

        final AtomicInteger counter;

        final AtomicInteger controller;

        final Map<Integer, Set<Integer>> grid;

        InsertPointTask(GridIndexPoint point, AtomicInteger counter, AtomicInteger controller, Map<Integer, Set<Integer>> grid) {
            this.point = point;
            this.counter = counter;
            this.controller = controller;
            this.grid = grid;
        }

        /**
         * insert the point
         * @see #insert(GridIndexPoint, Map)
         */
        @Override
        public void run() {
            controller.decrementAndGet();
            counter.incrementAndGet();
            if (counter.intValue() % 100000 == 0)
                logger.info("counter: {}, grid size: {}, queue size: {}", counter.intValue(), grid.size(), controller.intValue());
            insert(point, grid);
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
}
