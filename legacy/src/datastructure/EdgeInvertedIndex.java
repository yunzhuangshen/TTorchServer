package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.algorithm.LongestCommonSubsequence;
import au.edu.rmit.trajectory.similarity.model.*;
import au.edu.rmit.trajectory.similarity.util.CommonUtils;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Inverted dataStructure that refers to ﻿LEVI -- EdgII in paper.
 * It supports LORS( Longest Overlapping Road Segments)
 *
 * @author forrest0402
 * @date 1/3/2018
 */
public class EdgeInvertedIndex {

    private static Logger logger = LoggerFactory.getLogger(EdgeInvertedIndex.class);

    private final String INDEX_FILE1 = "dataStructure/EdgeIndex_T.idx";
    private final String INDEX_FILE2 = "dataStructure/EdgeIndex_P.idx";

    private final String INDEX_FILE_EDGE_ID = "dataStructure/EdgeIndex_G.idx";
    private final String INDEX_FILE1_C = "dataStructure/EdgeIndex_T.idx.compressed";
    private final String INDEX_FILE1_BI = "dataStructure/EdgeIndex_T.idx.bi.compressed";
    private final String INDEX_FILE2_C = "dataStructure/EdgeIndex_P.idx.compressed";

    private final static String SEPRATOR = ";";

    /**
     * key: Id of the edge
     * value: a List of entries of type EdgePair( key: trajectory hash
     *                                            value: edge position in this trajectory)
     */
    private Map<Integer, List<EdgePair<Integer>>> index;

    private Map<Integer, int[]> comIndexTrajID;

    private Map<Integer, byte[]> comIndexPositionList;

    private boolean loadCompressedIndex = false;

    @Autowired
    LongestCommonSubsequence longestCommonSubsequence;

    private IntegratedIntCompressor iic = new IntegratedIntCompressor();

    private ByteIntegerCODEC bic = new VariableByte();

    private long compressionTime;

    public void compress(boolean reorder) {
        CommonUtils.compress(INDEX_FILE1_BI, INDEX_FILE1_C, INDEX_FILE1, INDEX_FILE2, INDEX_FILE2_C, INDEX_FILE_EDGE_ID, reorder);
    }

    /**
     * buildTorGraph indexes that could query trajectories by edge.
     * save them to disk storage.
     *
     * output dataStructure files:
     * dataStructure/EdgeIndex_G.idx each line containing hash of a edge appear in trajectory set at once.          format: id1[\n]id2[\n]id3[\n]id4...
     * dataStructure/EdgeIndex_T.idx each line containing ids of trajectory having the edge separated by ";"    format: tra_id1;tra_id2...\n
     * dataStructure/EdgeIndex_P.idx each line containing position of the edge appearing in each trajectories     format: edge_tra_id1_pos;edge_tra_id2_pos...\n
     *
     *
     * @param trajectories a collection of instances of type Trajectory, containing information to buiding edge dataStructure
     * @param allEdges a map of edgeId-Edge pairs.
     */
    public void buildIndex(Collection<Trajectory> trajectories, Map<Integer, MMEdge> allEdges) {
        logger.info("Enter buildIndex");
        if (load()) return;

        Map<Integer, Set<EdgePair<Integer>>> index = new HashMap<>();
        for (Trajectory trajectory : trajectories) {
            List<MMEdge> edges = trajectory.getMapMatchedTrajectory(allEdges);
            int pos = 0;
            for (MMEdge edge : edges) {
                Set<EdgePair<Integer>> trajectoryIDSet = index.computeIfAbsent(edge.getId(), k -> new HashSet<>());
                trajectoryIDSet.add(new EdgePair(trajectory.getId(), ++pos));
            }
        }

        logger.info("store the dataStructure into the disk");
        File file = new File(INDEX_FILE1);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter idBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_EDGE_ID, false), StandardCharsets.UTF_8)));
             BufferedWriter trajBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE1, false), StandardCharsets.UTF_8)));
             BufferedWriter posBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE2, false), StandardCharsets.UTF_8)))) {
            for (Map.Entry<Integer, Set<EdgePair<Integer>>> entry : index.entrySet()) {
                //write edge hash
                idBufWriter.write(entry.getKey() + "");
                idBufWriter.newLine();
                List<EdgePair<Integer>> invertedList = entry.getValue().stream().sorted(Comparator.comparing(EdgePair<Integer>::getKey)).collect(Collectors.toList());
                for (EdgePair edgePair : invertedList) {
                    //write trjectory hash
                    trajBufWriter.write(edgePair.getKey() + SEPRATOR);
                    //write position
                    posBufWriter.write(edgePair.getValue() + SEPRATOR);
                }
                trajBufWriter.newLine();
                posBufWriter.newLine();
                trajBufWriter.flush();
                posBufWriter.flush();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        load();
        logger.info("Exit buildIndex");
    }

    /**
     * load edge dataStructure files from disk
     * the in-memory edge-dataStructure is an field of this instance
     * @see #index
     * @return true if the dataStructure file can be load and construct successfully
     *         false if indexes cannot be construct( cannot find dataStructure file or some other reasons)
     */
    public boolean load() {
        logger.info("Enter load");
        File file = new File(INDEX_FILE1);
        if (file.exists()) {
            logger.info("load from existing file \"{}\" and \"{}\"", INDEX_FILE1, INDEX_FILE2);
            index = new HashMap<>();
            try (BufferedReader idBufReader = new BufferedReader(new FileReader(INDEX_FILE_EDGE_ID));
                 BufferedReader trajBufReader = new BufferedReader(new FileReader(INDEX_FILE1));
                 BufferedReader posBufReader = new BufferedReader(new FileReader(INDEX_FILE2))) {
                String trajLine, posLine, idString;
                List<Integer> IDList = new ArrayList<>(100000);
                while ((idString = idBufReader.readLine()) != null) {
                    IDList.add(Integer.parseInt(idString));
                }
                int idx = 0;
                while ((trajLine = trajBufReader.readLine()) != null && (posLine = posBufReader.readLine()) != null) {
                    String[] trajArray = trajLine.split(SEPRATOR), posArray = posLine.split(SEPRATOR);
                    int edgeID = IDList.get(idx++);
                    List<EdgePair<Integer>> invertedList = index.get(edgeID);
                    if (invertedList == null) {
                        invertedList = new ArrayList<>();
                        index.put(edgeID, invertedList);
                    }
                    for (int i = 0; i < trajArray.length; i++) {
                        int trajectoryID = Integer.parseInt(trajArray[i]);
                        int pos = Integer.parseInt(posArray[i]);
                        invertedList.add(new EdgePair<>(trajectoryID, pos));
                    }
                }
                loadCompressedIndex = false;
                logger.info("load complete - " + index.size());
                return true;
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("edge dataStructure file doesn't exist");
        return false;
    }

    public boolean loadCompressedForm() {
        logger.info("Enter loadCompressedForm");
        File file = new File(INDEX_FILE1_C);
        comIndexTrajID = new HashMap<>();
        comIndexPositionList = new HashMap<>();
        if (file.exists()) {
            logger.info("load from existing file \"{}\" and \"{}\"", INDEX_FILE1_C, INDEX_FILE2_C);
            index = new HashMap<>();
            try (BufferedReader trajBufReader = new BufferedReader(new FileReader(INDEX_FILE1_C));
                 InputStream posBufReader = new FileInputStream(INDEX_FILE2_C);
                 BufferedReader gridIDReader = new BufferedReader(new FileReader(INDEX_FILE_EDGE_ID))) {
                List<Integer> gridID = new ArrayList<>();
                String lineStr;
                while ((lineStr = gridIDReader.readLine()) != null) {
                    gridID.add(Integer.parseInt(lineStr));
                }
                byte[] totalByteList = new byte[posBufReader.available()];
                posBufReader.read(totalByteList);
                int curGridID = 0;
                List<Byte> curByteList = new ArrayList<>();
                for (int i = 0; i < totalByteList.length; i++) {
                    byte value = totalByteList[i];
                    if (value == 0x1F && i + 3 < totalByteList.length
                            && totalByteList[i + 1] == 0x3F
                            && totalByteList[i + 2] == 0x5F
                            && totalByteList[i + 3] == 0x7F) {
                        byte[] byteArray = new byte[curByteList.size()];
                        for (int j = 0; j < curByteList.size(); j++) {
                            byteArray[j] = curByteList.get(j);
                        }
                        comIndexPositionList.put(gridID.get(curGridID++), byteArray);
                        curByteList = new ArrayList<>();
                        i += 3;
                    } else {
                        curByteList.add(value);
                    }
                }
                totalByteList = null;
                posBufReader.close();
                logger.info("start to load trajectory hash");
                int lineNum = 0;
                while ((lineStr = trajBufReader.readLine()) != null) {
                    String[] trajArray = lineStr.split(SEPRATOR);
                    int[] data = new int[trajArray.length];
                    for (int i = 0; i < trajArray.length; i++) {
                        data[i] = Integer.parseInt(trajArray[i]);
                    }
                    comIndexTrajID.put(gridID.get(lineNum++), data);
                }
                logger.info("loadCompressedForm complete");
                loadCompressedIndex = true;
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        logger.info("file doesn't exist");
        return false;
    }

    /**
     * LORS( Longest Overlapping Road Segments) algorithm.
     * Find top K trajectories that has the max score( similarity) against query trajectory( represented by a list of edges).
     * Used in efficiency test.
     *
     * @param querySegments a list of edges representing a query
     * @param k number of results returned
     * @param allEdges all the edges on virtual graph
     * @param restDistance a list containing the sum of rest edges length in total
     *                     example:
     *                     if the query contains 3 edges, which are 3 meters, 1 meters and 2 meters respectively in length.
     *                     Then the restDistance contains [3, 2, 0], which means that if it get to dataStructure 0, then the rest is 3.
     *                     If it get to dataStructure 1, then the rest is 2. And if it get to dataStructure 3, then the rest is 0.
     * @param candidateNumberList Instance of type List. Each entry containing the hash of candidate trajectory.
     *                            a candidate is a trajectory that contains at least one common edge with the query trajectory.
     *                            As this is for purpose of passing value, it will be modified in this method,
     * @param scannedCandidateNumberList Instance of type List. Each entry containing the hash of scanned trajectory.
     *                                   As this is for purpose of passing value, it will be modified in this method,
     * @param lookupTimeList Instance of type List. Each entry containing the time used for each query.
     *                       As this is for purpose of passing value, it will be modified in this method,
     * @param printLookupTime Boolean value, indicates if print the look up time( time used in process of find trajectories and location pair given a edgeId)
     * @param fullyScanNumList Instance of type List. Each entry containing the number of trajectories fully scanned.
     *                         As this is for purpose of passing value, it will be modified in this method,
     * @param compressedTime Instance of type List. Each entry containing the number of trajectories fully scanned.
     *                       As this is for purpose of passing value, it will be modified in this method.
     *
     * @return A list of results of type Integer meaning ids of trajectory.
     */
    public List<Integer> findTopK(List<Edge> querySegments, int k, Map<Integer, MMEdge> allEdges, double[] restDistance, List<Integer> candidateNumberList, List<Integer> scannedCandidateNumberList, List<Long> lookupTimeList, boolean printLookupTime, List<Integer> fullyScanNumList, List<Long> compressedTime) {

        if (index == null)
            throw new IllegalStateException("invoke buildTorGraph first");

        this.compressionTime = 0;

        long lookupStartDate = System.nanoTime();
        //List<TorSegment> querySegments = trajectory.getMapMatchedTrajectory(allEdges);
        //key for trajectory id, value for its upper bound
        Map<Integer, Double> upperBound = new HashMap<>();
        //key for trajectory id, value for the list of edges overlapped with the query edge.
        Map<Integer, List<Edge>> retrievedTrajMap = new HashMap<>();

        long lookupTime = 0;
        for (Edge queryEdge : querySegments) {
            List<EdgePair<Integer>> pairs;
            if (!printLookupTime) {
                pairs = find(queryEdge.getId());
            } else {
                long printLookupStartTime = System.nanoTime();
                pairs = find(queryEdge.getId());
                lookupTime += (System.nanoTime() - printLookupStartTime) / 1000000L;
            }
            if (pairs == null) continue;

            //key for trajectory hash, value for position
            for (EdgePair<Integer> pair : pairs) {

                //calculate upper bound for each trajectory
                Double value = upperBound.get(pair.getKey());
                if (value == null) {
                    upperBound.put(pair.getKey(), queryEdge.getLength());
                } else {
                    upperBound.put(pair.getKey(), queryEdge.getLength() + value);
                }

                //re-construct every trajectory, need to reorder in the next steps
                List<Edge> retrievedTrajectory = retrievedTrajMap.computeIfAbsent(pair.getKey(), k1 -> new ArrayList<>());
                retrievedTrajectory.add(new LightEdge(queryEdge.getId(), queryEdge.getLength(), pair.getValue()));
            }
        }

        long lookupEndDate = System.nanoTime();

        List<Integer> retList = new ArrayList<>();
        //key for trajectory hash, value for its upperbound
        PriorityQueue<Map.Entry<Integer, Double>> upperBoundRank = new PriorityQueue<>((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
        upperBoundRank.addAll(upperBound.entrySet());

        int candidateNumber = upperBoundRank.size();
        if (candidateNumberList != null)
            candidateNumberList.add(candidateNumber);

        double BestKthSoFar = -Integer.MAX_VALUE;
        PriorityQueue<Map.Entry<Integer, Double>> topKHeap = new PriorityQueue<>(Map.Entry.comparingByValue());

        int scannedCandidateNumber = 0;
        AtomicInteger fullyScannNumber = new AtomicInteger(0);

        while (!upperBoundRank.isEmpty()) {
            Map.Entry<Integer, Double> entry = upperBoundRank.poll();           //key-trajId, value-upper bound

            if (BestKthSoFar > entry.getValue() && topKHeap.size() >= k) break; //early termination

            ++scannedCandidateNumber;
            List<Edge> candidate = retrievedTrajMap.get(entry.getKey());
            candidate.sort(Comparator.comparingInt(Edge::getPosition));

            // todo
            double exactValue = longestCommonSubsequence.fastRun(querySegments, candidate, Integer.MAX_VALUE, restDistance, BestKthSoFar, fullyScannNumber);

            entry.setValue(exactValue);
            topKHeap.add(entry);
            if (topKHeap.size() > k) topKHeap.poll();
            BestKthSoFar = topKHeap.peek().getValue();
        }

        while (topKHeap.size() > 0) {
            retList.add(topKHeap.poll().getKey());
        }

        long searchEndDate = System.nanoTime();
        if (scannedCandidateNumberList != null) {
            scannedCandidateNumberList.add(scannedCandidateNumber);
        }
        if (lookupTimeList != null)
            lookupTimeList.add((lookupEndDate - lookupStartDate) / 1000000L);
        //logger.info("candidateNumber: {}, scannedCandidateNumber: {}, lookup time: {} ms, search time: {} ms", candidateNumber, scannedCandidateNumber, (lookupEndDate - lookupStartDate) / 1000000L, (searchEndDate - lookupEndDate) / 1000000L);
        if (printLookupTime) {
            logger.error("find time in lookup time is: {}", lookupTime);
        }
        if (fullyScanNumList != null)
            fullyScanNumList.add(fullyScannNumber.intValue());
        if (compressedTime != null)
            compressedTime.add(compressionTime / 1000000L);
        return retList;
    }

    /**
     * LORS( Longest Overlapping Road Segments) algorithm.
     * Find top K trajectories that has the max score( similarity) against query trajectory( represented by a list of edges).
     * Used in effectiveness test.
     */
    //todo
    public PriorityQueue<Map.Entry<Integer, Double>> findTopK(Map<Integer, Trajectory> trajectoryMap, List<MMEdge> querySegments, int k, Map<Integer, MMEdge> allEdges, double[] restDistance) {
        if (index == null)
            throw new IllegalStateException("invoke buildTorGraph first");
        double queryLength = len(querySegments);
        Map<Integer, Double> upperBound = new HashMap<>();
        //key for trajectory hash, value for ()
        for (Edge queryEdge : querySegments) {
            List<EdgePair<Integer>> pairs = find(queryEdge.getId());
            if (pairs == null) continue;
            //key for trajectory hash, value for position
            for (EdgePair<Integer> pair : pairs) {
                //calculate upper bound for each trajectory
                Double value = upperBound.get(pair.getKey());
                if (value == null) {
                    upperBound.put(pair.getKey(), queryEdge.getLength());
                } else {
                    if (queryEdge.getLength() + value >= queryLength)
                        upperBound.put(pair.getKey(), queryLength);
                    else upperBound.put(pair.getKey(), queryEdge.getLength() + value);
                }
            }
        }
        //key for trajectory hash, value for its upperbound
        PriorityQueue<Map.Entry<Integer, Double>> upperBoundRank = new PriorityQueue<>((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
        upperBoundRank.addAll(upperBound.entrySet());
        double bestSoFar = -Integer.MAX_VALUE;
        PriorityQueue<Map.Entry<Integer, Double>> topKHeap = new PriorityQueue<>(Map.Entry.comparingByValue());
        while (upperBoundRank.size() > 0) {
            Map.Entry<Integer, Double> entry = upperBoundRank.poll();
            if (bestSoFar > entry.getValue() && topKHeap.size() >= k)
                break;
            List<MMEdge> candidate = trajectoryMap.get(entry.getKey()).getMapMatchedTrajectory(allEdges);
            Collections.sort(candidate, (c1, c2) -> Integer.compare(c1.getPosition(), c2.getPosition()));
            double exactValue;
            if (topKHeap.size() == k)
                exactValue = longestCommonSubsequence.fastRun(querySegments, candidate, Integer.MAX_VALUE, restDistance, bestSoFar, null);
            else exactValue = longestCommonSubsequence.mmRun(querySegments, candidate, Integer.MAX_VALUE);
            entry.setValue(exactValue);
            topKHeap.add(entry);
            if (topKHeap.size() > k) topKHeap.poll();
            bestSoFar = topKHeap.peek().getValue();
        }
        return topKHeap;
    }

    /**
     * @param id
     */
    public List<EdgePair<Integer>> find(int id) {
        if (loadCompressedIndex) {
            long startTime = System.nanoTime();
            List<EdgePair<Integer>> res = new ArrayList<>();
            int[] compressedTrajIDArray = comIndexTrajID.get(id);
            if (compressedTrajIDArray != null) {
                int[] trajectoryIDArray = iic.uncompress(compressedTrajIDArray);
                byte[] compressedPositionArray = comIndexPositionList.get(id);
                int[] positionArray = new int[trajectoryIDArray.length];
                bic.uncompress(compressedPositionArray, new IntWrapper(), compressedPositionArray.length, positionArray, new IntWrapper());
                for (int i = 0; i < positionArray.length; i++) {
                    res.add(new EdgePair<>(trajectoryIDArray[i], positionArray[i]));
                }
            }
            compressionTime += (System.nanoTime() - startTime);
            return res;
        }
        return index.get(id);
    }

    public List<Integer> findRelevantTrajectoryID(int edgeID) {
        List<EdgePair<Integer>> relevantTrajList = index.get(edgeID);
        if (relevantTrajList == null)
            return new ArrayList<>();
        List<Integer> res = new ArrayList<>(relevantTrajList.size());
        for (EdgePair<Integer> edgePair : relevantTrajList) {
            res.add(edgePair.getKey());
        }
        return res;
    }

    public List<Integer> find(int edgeID, Map<Integer, Trajectory> trajectoryMap) {
        List<EdgePair<Integer>> pairs = find(edgeID);
        List<Integer> result = new ArrayList<>(pairs.size());
        if (pairs != null) {
            for (EdgePair<Integer> pair : pairs) {
                result.add(pair.getKey());
            }
        }
        return result;
    }

    public List<Integer> pathQuery(List<MMEdge> originalSegments, List<Long> compressedTime) {
        this.compressionTime = 0;
        Set<Integer> trajectoryID = new HashSet<>();
        for (MMEdge edge : originalSegments) {
            List<EdgePair<Integer>> pairs = find(edge.getId());
            if (pairs != null) {
                for (EdgePair<Integer> pair : pairs) {
                    trajectoryID.add(pair.getKey());
                }
            }
        }
        if (compressedTime != null)
            compressedTime.add(compressionTime);
        return new ArrayList<>(trajectoryID);
    }

    public List<Integer> strictPathQuery(List<MMEdge> originalSegments, List<Long> compressedTime) {
        this.compressionTime = 0;
        //key is trajectory hash, value is its position list
        Map<Integer, Set<Integer>> map = new HashMap<>();
        Set<Integer> preTrajectoryID = new HashSet<>(), curTrajectoryID = new HashSet<>();
        for (MMEdge edge : originalSegments) {
            List<EdgePair<Integer>> pairs = find(edge.getId());
            if (pairs != null) {
                for (EdgePair<Integer> pair : pairs) {
                    Set<Integer> pos = map.get(pair.getKey());
                    if (pos == null) {
                        pos = new HashSet<>();
                        map.put(pair.getKey(), pos);
                    }
                    pos.add(edge.getId());
                }
            }
        }
        List<Integer> result = new ArrayList<>();
        for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {
            if (entry.getValue().size() == originalSegments.size())
                result.add(entry.getKey());
        }
        if (compressedTime != null)
            compressedTime.add(compressionTime);
        return new ArrayList<>(result);
    }

    private double len(List<MMEdge> querySegments) {
        double len = 0;
        for (MMEdge querySegment : querySegments) {
            len += querySegment.getLength();
        }
        return len;
    }

    class LightEdge implements Edge {
        final int id;
        final double length;
        final int position;

        LightEdge(int id, double length, int position) {
            this.id = id;
            this.length = length;
            this.position = position;
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public double getLength() {
            return length;
        }

        public int getPosition() {
            return position;
        }
    }

    /**
     * key(Integer)--value(Integer) pair
     * @param <T> it must be something that extends Integer
     */
    class EdgePair<T extends Integer> implements Pair {

        final T key, value;

        /**
         * @param key trajectory hash
         * @param value position of the node in that trajectory
         */
        EdgePair(T key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public T getKey() {
            return key;
        }


        @Override
        public T getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EdgePair<?> edgePair = (EdgePair<?>) o;
            return Objects.equals(key, edgePair.key) &&
                    Objects.equals(value, edgePair.value);
        }

        @Override
        public int hashCode() {

            return Objects.hash(key, value);
        }
    }
}
