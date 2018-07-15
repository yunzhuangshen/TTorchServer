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
 * Grid index for FTSE
 *
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
@Component
public class GridIndex {

    private static Logger logger = LoggerFactory.getLogger(GridIndex.class);

    final String INDEX_FILE = "index/GridIndex.idx";

    final static String SEPRATOR = ";";

    float minLat = Float.MAX_VALUE, minLon = Float.MAX_VALUE, maxLat = -Float.MAX_VALUE, maxLon = -Float.MAX_VALUE, deltaLat, deltaLon, epsilon;

    int lengthNumber, widthNumber;

    /**
     * key for grid id, value for (trajectory id, position)
     */
    private Map<Integer, Map<Integer, List<Short>>> grids;

    /**
     * once index is built, it will be stored in the disk
     * return true if index can be loaded from disk
     * return false otherwise
     *
     * @return
     */
    public boolean load() {
        logger.info("Enter load");
        File file = new File(INDEX_FILE);
        String line = null;
        if (file.exists()) {
            this.minLat = 41.10894f;
            this.minLon = -8.70489f;
            this.maxLat = 41.222656f;
            this.maxLon = -8.489412f;
            this.deltaLat = 8.996529E-5f;
            this.deltaLon = 1.19577104E-4f;
            this.lengthNumber = 1802;
            this.widthNumber = 1264;
            try (BufferedReader bw = new BufferedReader(new FileReader(INDEX_FILE))) {
                //this.grids = new Grid[Integer.parseInt(line)];
                this.grids = new HashMap<>();
                while ((line = bw.readLine()) != null) {
                    String[] lineArray = line.split(SEPRATOR);
                    int gridPos = Integer.parseInt(lineArray[0]);
                    this.grids.put(gridPos, new HashMap<>());
                    for (int i = 1; i < lineArray.length; i++) {
                        String[] listArray = lineArray[i].split(" ");
                        int trajID = Integer.parseInt(listArray[0]);
                        List<Short> posList = new LinkedList<>();
                        for (int j = 1; j < listArray.length; j++) {
                            posList.add(Short.parseShort(listArray[j]));
                        }
                        this.grids.get(gridPos).put(trajID, posList);
                    }
                }
                logger.info("grid size: {}", this.grids.size());
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

    /**
     * Attention: crossing 0 is not supported such as (-50,100) (50,-100), the answer may be incorrect
     *
     * @param allPoints
     * @param _epsilon  the granularity of the grids (meters)
     */
    public void buildIndex(List<WrappedPoint> allPoints, float _epsilon) {
        logger.info("Enter buildIndex");
        this.epsilon = _epsilon;
        //find bounding
        minLat = Float.MAX_VALUE;
        minLon = Float.MAX_VALUE;
        maxLat = -Float.MAX_VALUE;
        maxLon = -Float.MAX_VALUE;
        for (WrappedPoint point : allPoints) {
            if (point.getLat() < minLat) minLat = (float) point.getLat();
            if (point.getLat() > maxLat) maxLat = (float) point.getLat();
            if (point.getLon() < minLon) minLon = (float) point.getLon();
            if (point.getLon() > maxLon) maxLon = (float) point.getLon();
        }

        //create grids
        double length = GeoUtil.distance(maxLat, maxLat, minLon, maxLon);
        double width = GeoUtil.distance(maxLat, minLat, minLon, minLon);
        this.lengthNumber = (int) (length / epsilon);
        this.widthNumber = (int) (width / epsilon);
        this.deltaLat = (maxLat - minLat) / this.widthNumber;
        this.deltaLon = (maxLon - minLon) / this.lengthNumber;
        logger.info("start to insert points, (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,lengthNumber,widthNumber)=({},{},{},{})  grid size: {}*{}={}, point size: {}", minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, lengthNumber, widthNumber, this.lengthNumber, this.widthNumber, this.lengthNumber * this.widthNumber, allPoints.size());
        grids = new HashMap<>(this.lengthNumber * this.widthNumber + 1);
        //insert points
        ExecutorService threadPool = new ThreadPoolExecutor(0, 15, 10000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        AtomicInteger process = new AtomicInteger(0);
        AtomicInteger controller = new AtomicInteger(100000);
        Iterator<WrappedPoint> iter = allPoints.iterator();
        while (iter.hasNext()) {
            WrappedPoint point = iter.next();
            while (controller.intValue() == 0) {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            threadPool.execute(new InsertPointTask(point, process, controller));
//            process.incrementAndGet();
//            if (process.intValue() % 50000 == 0)
//                logger.info("process: {}, grid size: {}, queue size: {}", process.intValue(), grids.size(), controller.intValue());
//            insert(point);
            iter.remove();
        }
        logger.info("start to shutdown");
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error("{}", e);
        }
        logger.info("start to sort");
        for (Map<Integer, List<Short>> map : grids.values()) {
            for (List<Short> list : map.values()) {
                Collections.sort(list, Comparator.naturalOrder());
            }
        }

        logger.info("store the index into the disk");
        File file = new File(INDEX_FILE);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter bw = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE, false), StandardCharsets.UTF_8)))) {
            //bw.write(this.grids.size());
            //bw.newLine();
            for (Map.Entry<Integer, Map<Integer, List<Short>>> gridEntry : grids.entrySet()) {
                Map<Integer, List<Short>> grid = gridEntry.getValue();
                if (grid != null) {
                    //write grid index
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
        logger.info("Exit buildIndex");
    }

    private int calculateGridID(double lat, double lon) {
        int row = (int) ((this.maxLat - lat) / this.deltaLat);
        int col = (int) ((lon - this.minLon) / this.deltaLon);
        return row * this.lengthNumber + col;
    }

    /**
     * key for trajectory id, value is the corresponding position
     *
     * @param point
     * @return
     */
    public Map<Integer, List<Short>> find(Point point) {
//        if (point.getLat() < minLat || point.getLat() > maxLat
//                || point.getLon() < minLon || point.getLon() > maxLon)
//            return null;
        int pos = calculateGridID(point.getLat(), point.getLon());
        if (!this.grids.containsKey(pos)) {
            //Attention, this is possible, because that grid doesn't contain any points
            logger.error("pos = {}, point=({},{}) (minLat,minLon)=({},{}), (maxLat,maxLon)=({},{}), (deltaLat,deltaLon,lengthNumber,widthNumber)=({},{},{},{})  grid size: {}*{}={}", pos, point.getLat(), point.getLon(), minLat, minLon, maxLat, maxLon, deltaLat, deltaLon, lengthNumber, widthNumber, this.lengthNumber, this.widthNumber, this.lengthNumber * this.widthNumber, this.grids.size());
        }
        return this.grids.get(pos);
    }

    private void insert(double lat, double lon, int trajectoryID, short position, Set<Integer> positionSet) {
        synchronized (GridIndex.class) {
            if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) return;
            int pos = calculateGridID(lat, lon);
            if (positionSet.contains(pos)) return;
            positionSet.add(pos);
            if (this.grids.get(pos) == null) {
                this.grids.put(pos, new HashMap<>());
            }
            List<Short> posList = this.grids.get(pos).get(trajectoryID);
            if (posList == null) {
                posList = new LinkedList<>();
                posList.add(position);
                this.grids.get(pos).put(trajectoryID, posList);
            } else posList.add(position);
        }
    }

    /**
     * for one point, 9 points should be insert
     *
     * @param point
     */
    public void insert(WrappedPoint point) {
        double lowerLat = GeoUtil.increaseLatitude(point.getLat(), -epsilon);
        double upperLat = GeoUtil.increaseLatitude(point.getLat(), epsilon);
        double lowerLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -epsilon);
        double upperLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), epsilon);

        Set<Integer> positionSet = new HashSet<>();
        insert(upperLat, lowerLon, point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(upperLat, point.getLon(), point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(upperLat, upperLon, point.getTrajectoryId(), point.getPosition(), positionSet);

        insert(point.getLat(), lowerLon, point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(point.getLat(), point.getLon(), point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(point.getLat(), upperLon, point.getTrajectoryId(), point.getPosition(), positionSet);

        insert(lowerLat, lowerLon, point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(lowerLat, point.getLon(), point.getTrajectoryId(), point.getPosition(), positionSet);
        insert(lowerLat, upperLon, point.getTrajectoryId(), point.getPosition(), positionSet);
        positionSet.clear();
    }

    public void delete() {
        throw new UnsupportedOperationException("");
    }

    class InsertPointTask implements Runnable {

        final WrappedPoint point;

        final AtomicInteger process;

        final AtomicInteger controller;

        InsertPointTask(WrappedPoint point, AtomicInteger process, AtomicInteger controller) {
            this.point = point;
            this.process = process;
            this.controller = controller;
        }

        @Override
        public void run() {
            controller.decrementAndGet();
            process.incrementAndGet();
            if (process.intValue() % 100000 == 0)
                logger.info("process: {}, grid size: {}, queue size: {}", process.intValue(), grids.size(), controller.intValue());
            insert(point);
            controller.incrementAndGet();
        }
    }
}
