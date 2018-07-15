package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.model.*;
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
 * Grid dataStructure for FTSE
 * It is for indexing all the ids of trajectory and its corresponding nodes into the grid.
 * It is possible that same traId appears in different tiles which is the case of trajectory across tiles.
 *
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
@Component
public class ComplexTraGridIndex {

    private static Logger logger = LoggerFactory.getLogger(ComplexTraGridIndex.class);

    private final String INDEX_FILE = "dataStructure/GridIndex.idx";

    private final static String SEPRATOR = ";";

    private float minLat = Float.MAX_VALUE, minLon = Float.MAX_VALUE, maxLat = -Float.MAX_VALUE, maxLon = -Float.MAX_VALUE, deltaLat, deltaLon, epsilon;

    private int horizontalTileNumber, verticalTileNumber;

    /**
     * key for tile hash, value for (trajectory hash, a list of position)
     * The data structure enables to find points of trajectory within a single tile.
     */
    private Map<Integer, Map<Integer, List<Short>>> grid;

    /**
     * Attention: crossing 0 is not supported such as (-50,100) (50,-100), the answer may be incorrect
     *
     * @param allPointsOnTra A list of instances of type GridIndexPoint.
     *                       Note that the points contained by more than one trajectories should be appear more than one time.
     * @param _epsilon  The granularity of a tile (meters)
     */
    public void buildIndex(List<GridIndexPoint> allPointsOnTra, float _epsilon) {
        logger.info("Enter buildIndex");
        this.epsilon = _epsilon;
        
        //find bounding
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
        double horizontal_span = GeoUtil.distance(maxLat, maxLat, minLon, maxLon);
        double vertical_span = GeoUtil.distance(maxLat, minLat, minLon, minLon);
        this.horizontalTileNumber = (int) (horizontal_span / epsilon);
        this.verticalTileNumber = (int) (vertical_span / epsilon);
        this.deltaLat = (maxLat - minLat) / this.verticalTileNumber;
        this.deltaLon = (maxLon - minLon) / this.horizontalTileNumber;
        
        logger.info("start to insert points, (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}, point size: {}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, allPointsOnTra.size());
        grid = new HashMap<>(this.horizontalTileNumber * this.verticalTileNumber + 1);
        //insert points
        ExecutorService threadPool = new ThreadPoolExecutor(0, 15, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger controller = new AtomicInteger(100000);
        Iterator<GridIndexPoint> itr = allPointsOnTra.iterator();
        while (itr.hasNext()) {
            GridIndexPoint point = itr.next();
            while (controller.intValue() == 0) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            threadPool.execute(new InsertPointTask(point, counter, controller));
            //itr.remove();
        }
        logger.info("start to shutdown");
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("start to sort");
        for (Map<Integer, List<Short>> map : grid.values()) {
            for (List<Short> list : map.values()) {
                list.sort(Comparator.naturalOrder());
            }
        }

        logger.info("store the dataStructure into the disk");
        File file = new File(INDEX_FILE);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter bw = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE, false), StandardCharsets.UTF_8)))) {
            bw.write(this.minLat + SEPRATOR);
            bw.write(this.minLon + SEPRATOR);
            bw.write(this.maxLat + SEPRATOR);
            bw.write(this.maxLon + SEPRATOR);
            bw.write(this.deltaLat + SEPRATOR);
            bw.write(this.deltaLon + SEPRATOR);
            bw.write(this.horizontalTileNumber + SEPRATOR);
            bw.write(this.verticalTileNumber + SEPRATOR);
            bw.write(this.epsilon + SEPRATOR);
            bw.newLine();
            for (Map.Entry<Integer, Map<Integer, List<Short>>> gridEntry : grid.entrySet()) {
                Map<Integer, List<Short>> grid = gridEntry.getValue();
                if (grid != null) {
                    //write grid dataStructure
                    bw.write(gridEntry.getKey() + "");
                    for (Map.Entry<Integer, List<Short>> entry : grid.entrySet()) {
                        bw.write(SEPRATOR);
                        bw.write(entry.getKey() + " ");
                        //position
                        boolean firstPos = true;
                        for (Short pos : entry.getValue()) {
                            if (!firstPos) bw.write(" ");
                            else firstPos = false;
                            bw.write(pos + "");
                        }
                    }
                    bw.newLine();
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
        File file = new File(INDEX_FILE);
        String line = null;
        if (file.exists()) {
            this.minLat = 39.58745f;
            this.minLon = 115.82537f;
            this.maxLat = 40.33455f;
            this.maxLon = 116.7893f;
            this.deltaLat = 2.2489489E-4f;
            this.deltaLon = 2.9495967E-4f;
            this.horizontalTileNumber = 8170;
            this.verticalTileNumber = 8307;

            try (BufferedReader bw = new BufferedReader(new FileReader(INDEX_FILE))) {
                line = bw.readLine();
                String[] trajLineArray = line.split(SEPRATOR);
                this.minLat = Float.parseFloat(trajLineArray[0]);
                this.minLon = Float.parseFloat(trajLineArray[1]);
                this.maxLat = Float.parseFloat(trajLineArray[2]);
                this.maxLon = Float.parseFloat(trajLineArray[3]);
                this.deltaLat = Float.parseFloat(trajLineArray[4]);
                this.deltaLon = Float.parseFloat(trajLineArray[5]);
                this.horizontalTileNumber = Integer.parseInt(trajLineArray[6]);
                this.verticalTileNumber = Integer.parseInt(trajLineArray[7]);
                this.epsilon = Float.parseFloat(trajLineArray[8]);
                logger.info("(minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber);
                this.grid = new HashMap<>();
                while ((line = bw.readLine()) != null) {
                    String[] lineArray = line.split(SEPRATOR);
                    int gridPos = Integer.parseInt(lineArray[0]);
                    this.grid.put(gridPos, new HashMap<>());
                    for (int i = 1; i < lineArray.length; i++) {
                        String[] listArray = lineArray[i].split(" ");
                        int trajID = Integer.parseInt(listArray[0]);
                        List<Short> posList = new LinkedList<>();
                        for (int j = 1; j < listArray.length; j++) {
                            posList.add(Short.parseShort(listArray[j]));
                        }
                        this.grid.get(gridPos).put(trajID, posList);
                    }
                }
                logger.info("grid size: {}", this.grid.size());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println(line);
            }
            logger.info("Exit load");
            return true;
        }
        logger.info("Exit load");
        return false;
    }

    private int calculateTileID(double lat, double lon) {
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
    public Map<Integer, List<Short>> find(Point point) {
//        if (point.getLat() < minLat || point.getLat() > maxLat
//                || point.getLon() < minLon || point.getLon() > maxLon)
//            return null;
        int tileId = calculateTileID(point.getLat(), point.getLon());
        if (!this.grid.containsKey(tileId)) {
            //Attention, this is possible, because that grid doesn't contain any points
            logger.error("tileId = {}, point=({},{}) (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,horizontalTileNumber,verticalTileNumber)=({},{},{},{})  grid size: {}*{}={}", tileId, point.getLat(), point.getLon(), minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, horizontalTileNumber, verticalTileNumber, this.horizontalTileNumber, this.verticalTileNumber, this.horizontalTileNumber * this.verticalTileNumber, this.grid.size());
        }
        return this.grid.get(tileId);
    }

    private void insert(double lat, double lon, int trajectoryID, short position, Set<Integer> tileIdSet) {
        synchronized (ComplexTraGridIndex.class) {
            if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) return;
            int tileId = calculateTileID(lat, lon);
            if (tileIdSet.contains(tileId)) return;

            tileIdSet.add(tileId);
            Map<Integer, List<Short>> tra_pos_map = this.grid.computeIfAbsent(tileId, k -> new HashMap<>());

            List<Short> posList = tra_pos_map.computeIfAbsent(trajectoryID, k -> new LinkedList<>());
            posList.add(position);
            tra_pos_map.put(trajectoryID, posList);
        }
    }

    /**
     * for one point, 9 points should be insert
     *
     * @param point
     */
    public void insert(GridIndexPoint point) {
        double lowerLat = GeoUtil.increaseLatitude(point.getLat(), -epsilon);
        double upperLat = GeoUtil.increaseLatitude(point.getLat(), epsilon);
        double lowerLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -epsilon);
        double upperLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), epsilon);
        double originLat = point.getLat();
        double originLng = point.getLon();

        int TraId = point.getTrajectoryId();
        short pos = point.getPosition();

        Set<Integer> tileIdSet = new HashSet<>();
        insert(upperLat, lowerLon, TraId, pos, tileIdSet);
        insert(upperLat, originLng, TraId, pos, tileIdSet);
        insert(upperLat, upperLon, TraId, pos, tileIdSet);

        insert(originLat, lowerLon, TraId, pos, tileIdSet);
        insert(originLat, originLng, TraId, pos, tileIdSet);
        insert(originLat, upperLon, TraId, pos, tileIdSet);

        insert(lowerLat, lowerLon, TraId, pos, tileIdSet);
        insert(lowerLat, originLng, TraId, pos, tileIdSet);
        insert(lowerLat, upperLon, TraId, pos, tileIdSet);
        tileIdSet.clear();
    }

    public void delete() {
        throw new UnsupportedOperationException("");
    }

    class InsertPointTask implements Runnable {

        final GridIndexPoint point;

        final AtomicInteger counter;

        final AtomicInteger controller;

        InsertPointTask(GridIndexPoint point, AtomicInteger counter, AtomicInteger controller) {
            this.point = point;
            this.counter = counter;
            this.controller = controller;
        }

        @Override
        public void run() {
            controller.decrementAndGet();
            counter.incrementAndGet();
            if (counter.intValue() % 100000 == 0)
                logger.info("counter: {}, grid size: {}, queue size: {}", counter.intValue(), grid.size(), controller.intValue());
            insert(point);
            controller.incrementAndGet();
        }
    }
}
