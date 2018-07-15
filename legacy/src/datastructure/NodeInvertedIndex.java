package au.edu.rmit.trajectory.similarity.datastructure;

import au.edu.rmit.trajectory.similarity.model.MMPoint;
import au.edu.rmit.trajectory.similarity.model.Pair;
import au.edu.rmit.trajectory.similarity.model.Point;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.task.MeasureType;
import au.edu.rmit.trajectory.similarity.util.CommonUtils;
import au.edu.rmit.trajectory.similarity.util.GeoUtil;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author forrest0402
 * @Description
 * @date 1/3/2018
 */
@Component
public class NodeInvertedIndex {

    private static Logger logger = LoggerFactory.getLogger(NodeInvertedIndex.class);

    //key for node hash, value for corresponding trajectories (trajectory hash, pos)
    private Map<Integer, List<NodePair<Integer>>> index;

    //key is node hash, value is compressed trajectory hash list
    private Map<Integer, int[]> comIndexTrajID;

    private Map<Integer, byte[]> comIndexNodeTraPos;
    /**
     * format: trajectory hash 1, trajectory hash 2, ...
     */
    private static final String INDEX_FILE_TRA_ID = "dataStructure/NodeIndex_T.idx";

    /**
     * format: position of trajectory hash 1, position of trajectory hash 2, ...
     */
    private static final String INDEX_FILE_TRA_POS = "dataStructure/NodeIndex_P.idx";

    /**
     * format: node hash, node hash, ...
     */
    private static final String INDEX_FILE_NODE_ID = "dataStructure/NodeIndex_ID.idx";

    private static final String INDEX_FILE_TRA_ID_COMPRESSED = "dataStructure/NodeIndex_T.idx.compressed";
    private static final String INDEX_FILE_TRA_POS_COMPRESSED = "dataStructure/NodeIndex_P.idx.compressed";
    private static final String INDEX_FILE1_BI = "dataStructure/NodeIndex_T.idx.bi.compressed";
    private boolean loadCompressedIndex = false;
    private long compressionTime;
    private IntegratedIntCompressor iic = new IntegratedIntCompressor();

    private ByteIntegerCODEC bic = new VariableByte();
    private final static String SEPRATOR = ";";

    public void compress(boolean reorder) {
        CommonUtils.compress(INDEX_FILE1_BI, INDEX_FILE_TRA_ID_COMPRESSED, INDEX_FILE_TRA_ID, INDEX_FILE_TRA_POS, INDEX_FILE_TRA_POS_COMPRESSED, INDEX_FILE_NODE_ID, reorder);
    }


    /**
     * Build inverted dataStructure where key = node hash and value = (traId, position of the node in the trajectory) tuple
     * Save the dataStructure to file.
     *
     * output files:
     * - dataStructure/NodeIndex_ID.idx: containing ids of node that appears in the trajectory data set.
     *                           format: id1[newline character]id2[newline character]id3[newline character]...
     *
     * - dataStructure/NodeIndex_T.idx: containing ids of trajectories corresponding the same node.
     *                          format for each line: id1[;]id2[;]id3[;]...[newline character]
     *
     * - dataStructure/NodeIndex_P.idx containing positions of each trajectory where that node resides.
     *                         format for each line: pos1[;]pos2[;]pos3[;]...[newline character]
     *
     * @param trajectories a collection of calibrated trajectories
     */
    public void buildIndex(Collection<Trajectory> trajectories) {
        logger.info("Enter buildIndex");
        if (load()) return;
        Map<Integer, Set<NodePair<Integer>>> index = new HashMap<>();
        for (Trajectory trajectory : trajectories) {
            List<MMPoint> mapMatchedPoints = trajectory.getMMPoints();
            int pos = 0;
            for (MMPoint point : mapMatchedPoints) {
                Set<NodePair<Integer>> trajectoryIDSet = index.computeIfAbsent(point.getId(), k -> new HashSet<>());
                trajectoryIDSet.add(new NodePair<>(trajectory.getId(), ++pos));
            }
        }

        logger.info("store the dataStructure into the disk");
        File file = new File(INDEX_FILE_TRA_ID);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
        }
        try (BufferedWriter idBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_NODE_ID, false), StandardCharsets.UTF_8)));
             BufferedWriter trajBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_TRA_ID, false), StandardCharsets.UTF_8)));
             BufferedWriter posBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(INDEX_FILE_TRA_POS, false), StandardCharsets.UTF_8)))) {
            for (Map.Entry<Integer, Set<NodePair<Integer>>> entry : index.entrySet()) {
                //write point hash
                idBufWriter.write(entry.getKey() + "");
                idBufWriter.newLine();
                List<NodePair<Integer>> invertedList = entry.getValue().stream().sorted(Comparator.comparing(NodePair<Integer>::getKey)).collect(Collectors.toList());
                for (NodePair<Integer> edgePair : invertedList) {
                    //write trjectory hash
                    trajBufWriter.write(edgePair.getKey() + SEPRATOR);
                    //write position
                    posBufWriter.write(edgePair.getValue() + SEPRATOR);
                }
                trajBufWriter.newLine();
                posBufWriter.newLine();
            }
            trajBufWriter.flush();
            posBufWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit buildIndex");
    }

    /**
     * load the node non-compressed inverted indexes files to memory.
     *
     * @see #index data structure to be loaded to in the counter
     * @see #buildIndex(Collection)
     * @return true if dataStructure can be loaded from disk
     *         false otherwise
     */
    public boolean load() {
        logger.info("Enter load");
        File file = new File(INDEX_FILE_TRA_ID);
        if (index == null && file.exists()) {
            logger.info("{} file exist", INDEX_FILE_TRA_ID);
            index = new HashMap<>();
            try (BufferedReader idBufReader = new BufferedReader(new FileReader(INDEX_FILE_NODE_ID));
                 BufferedReader trajBufReader = new BufferedReader(new FileReader(INDEX_FILE_TRA_ID));
                 BufferedReader posBufReader = new BufferedReader(new FileReader(INDEX_FILE_TRA_POS))) {
                String trajLine, posLine, idString;
                List<Integer> IDList = new ArrayList<>(100000);
                while ((idString = idBufReader.readLine()) != null) {
                    //IDList.add(Integer.parseInt(idString.replace(";", "")));
                    IDList.add(Integer.parseInt(idString));
                }
                int idx = 0;
                while ((trajLine = trajBufReader.readLine()) != null && (posLine = posBufReader.readLine()) != null) {
                    String[] trajArray = trajLine.split(SEPRATOR), posArray = posLine.split(SEPRATOR);
                    int pointID = IDList.get(idx++);
                    List<NodePair<Integer>> invertedList = index.computeIfAbsent(pointID, k -> new ArrayList<>());

                    for (int i = 0; i < trajArray.length; i++) {
                        int trajectoryID = Integer.parseInt(trajArray[i]);
                        int pos = Integer.parseInt(posArray[i]);
                        if (trajectoryID < 1800000)
                            invertedList.add(new NodePair<>(trajectoryID, pos));
                    }
                }
                logger.info("load complete - " + index.size());
                return true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.info("Exit load");
        return false;
    }

    /**
     * load the node compressed inverted indexes files to memory.
     *
     * @see #comIndexTrajID data structure to be loaded to in the counter
     * @see #comIndexNodeTraPos data structure to be loaded to in the counter
     * @return true if dataStructure can be loaded from disk
     *         false otherwise
     */
    public boolean loadCompressedForm() {
        logger.info("Enter loadCompressedForm");
        File file = new File(INDEX_FILE_TRA_ID_COMPRESSED);
        comIndexTrajID = new HashMap<>();
        comIndexNodeTraPos = new HashMap<>();
        if (file.exists()) {
            logger.info("load from existing file \"{}\" and \"{}\"", INDEX_FILE_TRA_ID_COMPRESSED, INDEX_FILE_TRA_POS_COMPRESSED);
            index = new HashMap<>();
            try (BufferedReader trajBufReader = new BufferedReader(new FileReader(INDEX_FILE_TRA_ID_COMPRESSED));
                 InputStream posInputStream = new FileInputStream(INDEX_FILE_TRA_POS_COMPRESSED);
                 BufferedReader nodeIDReader = new BufferedReader(new FileReader(INDEX_FILE_NODE_ID))) {

                //read node ids from nodeId file to list.
                List<Integer> nodeIdList = new ArrayList<>();
                String lineStr;
                while ((lineStr = nodeIDReader.readLine()) != null) {
                    nodeIdList.add(Integer.parseInt(lineStr));
                }

                //read position information from compressed position file
                byte[] bytes = new byte[posInputStream.available()];
                posInputStream.read(bytes);
                int curGridID = 0;
                List<Byte> curByteList = new ArrayList<>();
                for (int i = 0; i < bytes.length; i++) {
                    byte value = bytes[i];
                    if (value == 0x1F && i + 3 < bytes.length
                            && bytes[i + 1] == 0x3F
                            && bytes[i + 2] == 0x5F
                            && bytes[i + 3] == 0x7F) {
                        byte[] byteArray = new byte[curByteList.size()];
                        for (int j = 0; j < curByteList.size(); j++) {
                            byteArray[j] = curByteList.get(j);
                        }
                        comIndexNodeTraPos.put(nodeIdList.get(curGridID++), byteArray);
                        curByteList = new ArrayList<>();
                        i += 3;
                    } else {
                        curByteList.add(value);
                    }
                }
                bytes = null;
                posInputStream.close();

                logger.info("start to load trajectory hash");
                int lineNum = 0;
                while ((lineStr = trajBufReader.readLine()) != null) {
                    String[] trajArray = lineStr.split(SEPRATOR);
                    int[] data = new int[trajArray.length];
                    for (int i = 0; i < trajArray.length; i++) {
                        data[i] = Integer.parseInt(trajArray[i]);
                    }
                    comIndexTrajID.put(nodeIdList.get(lineNum++), data);
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

    public List<Integer> rangeQuery(NodeGridIndex nodeGridIndex, MMPoint point, double r, List<Long> compressedTime) {
        compressionTime = 0;
        double minlat = GeoUtil.increaseLatitude(point.getLat(), -r);
        double minLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), -r);
        double maxLat = GeoUtil.increaseLatitude(point.getLat(), r);
        double maxLon = GeoUtil.increaseLongtitude(point.getLat(), point.getLon(), r);
        Collection<Integer> pointIDList = nodeGridIndex.findRange(minlat, minLon, maxLat, maxLon);
        Set<Integer> trajID = new HashSet<>();
        if (pointIDList != null) {
            for (Integer pointID : pointIDList) {
                List<NodePair<Integer>> list = find(pointID);
                if (list == null) continue;
                for (NodePair<Integer> pair : list) {
                    trajID.add(pair.getKey());
                }
            }
        }
        if (compressedTime != null)
            compressedTime.add(compressionTime);
        return new ArrayList<>(trajID);
    }

    /**
     * The method finds top k most similar trajectories given one query trajectory, based on LEVI dataStructure.
     * It supports LCSS and EDR.
     *
     * @param nodeGridIndex grid dataStructure that indexes all the nodes.
     * @param queryTraj the query which is essential a trajectory.
     * @param k number of results expected
     * @param measureType it could be LCSS or EDR
     * @param trajLenMap key: trajectory hash, value: length of the trajectory
     * @param candidateNumberList  for purpose of passing value
     * @param scannedCandidateNumberList for purpose of passing value
     * @param lookupTimeList for purpose of passing value
     * @param compressedTime for purpose of passing value
     * @return a list of results represented by hash of trajectory.
     */
    public List<Integer> findTopK(NodeGridIndex nodeGridIndex, Trajectory queryTraj, int k, MeasureType measureType,
                                  Map<Integer, Short> trajLenMap, List<Integer> candidateNumberList, List<Integer> scannedCandidateNumberList,
                                  List<Long> lookupTimeList, List<Long> compressedTime) {
        if (index == null)
            throw new IllegalStateException("invoke buildTorGraph first");

        //find all the candidate trajectories.
        //construct bounds map and trajPosMap map for refine step.
        long lookupStart = System.nanoTime();

        List<MMPoint> queryPoints = queryTraj.getMMPoints();

        Map<Integer, Integer> upperBound = new HashMap<>();                         //key for trajectory hash, value for its upper bound score
        Map<Integer, Map<Integer, List<Integer>>> trajPosMap = new HashMap<>();     //key: trajectory hash, value: map( key: query point position, value: a list of matched position of that trajectory)

        int position = 0, n = queryPoints.size();
        int candidateNumber = 0, scannedCandidateNumber = 0;
        compressionTime = 0;

        for (MMPoint queryPoint : queryPoints) {
            List<NodePair<Integer>> pairs = find(nodeGridIndex, queryPoint);        //key: trajectory hash -- value: position

            for (NodePair<Integer> pair : pairs) {
                Integer value = upperBound.get(pair.getKey());

                Map<Integer, List<Integer>> L = null;                               //key for position of query point, value for position list of candidate
                if (value == null) {
                    upperBound.put(pair.getKey(), 1);
                    L = new HashMap<>();
                    trajPosMap.put(pair.getKey(), L);
                } else {
                    upperBound.put(pair.getKey(), value + 1);
                    L = trajPosMap.get(pair.getKey());
                }

                List<Integer> posList = L.computeIfAbsent(position, k1 -> new ArrayList<>());
                posList.add(pair.getValue());
            }
            ++position;
        }

        //refine bound for EDR algorithm( edit distance on real sequence)
        if (measureType == MeasureType.EDR) {
            for (Map.Entry<Integer, Integer> entry : upperBound.entrySet()) {
                // compute maximum number of common points
                int max = Math.min(queryPoints.size(), trajLenMap.get(entry.getKey()));
                max = Math.min(max, entry.getValue());
                // compute minimum number of edits required to make two same.
                int min = Math.max(queryPoints.size(), entry.getValue()) - max;
                entry.setValue(min);
            }
        }

        //refine bound for LCSS algorithm( longest common sub-sequence)
        if (measureType == MeasureType.LCSS) {
            for (Map.Entry<Integer, Integer> entry : upperBound.entrySet()) {
                    int minLen = Math.min(queryPoints.size(), trajLenMap.get(entry.getKey()));
                    minLen = Math.min(minLen, entry.getValue());
                    entry.setValue(minLen);
            }
        }
        long lookupEnd = System.nanoTime();



        List<Integer> resIDList = new ArrayList<>();
        //key for queryTraj hash, value for its upper bound
        List<Map.Entry<Integer, Integer>> upperBoundRank = new ArrayList<>(upperBound.entrySet());
        //for LCSS algorithm, sort the bound from highest to lowest
        if (measureType == MeasureType.LCSS)
            upperBoundRank.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));
        //for EDR algorithm, sort the bound from lowest to highest
        if (measureType == MeasureType.EDR)
            upperBoundRank.sort(Map.Entry.comparingByValue());

        int bestKthSoFar = Integer.MIN_VALUE;
        PriorityQueue<Map.Entry<Integer, Integer>> topKHeap = null;
        //for LCSS, construct priority queue that always poll lowest value.
        if (measureType == MeasureType.LCSS)
            topKHeap = new PriorityQueue<>(Map.Entry.comparingByValue());
        //for EDR, construct priority queue that always poll highest value.
        else
            topKHeap = new PriorityQueue<>((c1, c2) -> c2.getValue().compareTo(c1.getValue()));


        candidateNumber = upperBoundRank.size();
        Set<Integer> ranked = new HashSet<>();
        for (Map.Entry<Integer, Integer> entry : upperBoundRank) {

            //define stop condition
            //for LCSS, the stop condition is that the Kth element score is no less than the upper bound of un-scanned trajectory
            if (measureType == MeasureType.LCSS) {
                if (bestKthSoFar >= entry.getValue() && topKHeap.size() >= k)
                    break;
            //for EDR, the stop condition is that the Kth element edit distance is no larger than the upper bound( minimum edits) of un-scanned trajectory
            } else {
                if (bestKthSoFar <= entry.getValue() && topKHeap.size() >= k)
                    break;
            }


            int candidateTrajectoryId = entry.getKey();
            // key: position of the query point
            // value: a list of position of matched point in trajectory.
            Map<Integer, List<Integer>> L = trajPosMap.get(candidateTrajectoryId);
            if (!ranked.contains(candidateTrajectoryId)) {
                ranked.add(candidateTrajectoryId);
                for (List<Integer> posList : L.values()) {
                    if (posList.size() > 1) {
                        posList.sort(Comparator.naturalOrder());
                    }
                }
            }
            ++scannedCandidateNumber;
            if (candidateTrajectoryId == 1510433)
                System.out.print("");


            int exactValue = 0;
            try {
                switch (measureType) {
                    case LCSS:
                        exactValue = lcssAlg(trajLenMap.get(entry.getKey()), n, L);
                        break;
                    case EDR:
                        exactValue = edrAlg(trajLenMap.get(entry.getKey()), n, L);
                        break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            entry.setValue(exactValue);
            topKHeap.add(entry);
            if (topKHeap.size() > k) topKHeap.poll();
            bestKthSoFar = topKHeap.peek().getValue();
        }


        while (!topKHeap.isEmpty()) {
            resIDList.add(topKHeap.poll().getKey());
        }
        if (candidateNumberList != null) {
            candidateNumberList.add(candidateNumber);
            logger.info("candidateNumber: {}, scannedCandidateNumber: {}", candidateNumber, scannedCandidateNumber);
        }
        if (lookupTimeList != null)
            lookupTimeList.add((lookupEnd - lookupStart) / 1000000L);
        if (scannedCandidateNumberList != null)
            scannedCandidateNumberList.add(scannedCandidateNumber);
        if (compressedTime != null)
            compressedTime.add(compressionTime / 1000000L);
        return resIDList;
    }

    /**
     * longest Common Subsequence algorithm implementation
     *
     * @param m the length of candidate trajectory
     * @param n the length of query trajectory
     * @param L key: position of the query point
     *          value: a list of position of matched point against position of query point(key) in trajectory.
     * @return maximum number of matches between two trajectories.
     */
    public static int lcssAlg(int m, int n, Map<Integer, List<Integer>> L) {
        int[] matches = new int[n + 1];

        for (int i = 1; i < matches.length; ++i)
            matches[i] = m + 1;

        int max = 0;
        for (int i = 0; i < n; ++i) {
            int temp = matches[0], c = 0;
            List<Integer> posList = L.get(i);
            if (posList == null) continue;
            for (Integer k : posList) {
                if (temp < k) {
                    while (matches[c] < k) ++c;
                    temp = matches[c];
                    matches[c] = k;
                    if (c > max)
                        max = c;
                }
            }
        }

        return max;
    }

    /**
     * edit distance on real sequence algorithm implementation
     *
     * @param m the length of candidate trajectory
     * @param n the length of query trajectory
     * @param L key: position of the query point
     *          value: a list of position of matched point against position of query point(key) in trajectory.
     * @return minimum number of edits between two trajectories.
     */
    private int edrAlg(int m, int n, Map<Integer, List<Integer>> L) {
        int[] matches = new int[2 * n + 2];
        for (int i = 1; i < matches.length; ++i)
            matches[i] = m + 1;
        int max = 0;
        for (int i = 1; i <= n; ++i) {
            int temp = matches[0], temp2 = matches[0], c = 0;
            List<Integer> posList = L.get(i - 1);
            if (posList == null) continue;
            for (Integer k : posList) {
                if (temp < k) {
                    while (matches[c] < k) {
                        if (temp < matches[c] - 1 && temp < m - 1) {
                            temp2 = matches[c];
                            matches[c] = temp + 1;
                            temp = temp2;
                        } else temp = matches[c];
                        c++;
                    }
                    temp2 = matches[c];
                    matches[c] = temp + 1;
                    temp = matches[c + 1];
                    if (matches[c + 1] > k) matches[c + 1] = k;
                    if (max < c + 1) max = c + 1;
                    c += 2;
                } else if (temp2 < k && k < matches[c]) {
                    temp2 = temp;
                    temp = matches[c];
                    matches[c] = k;
                    if (max < c) max = c;
                    ++c;
                }
            }
            for (int j = c; j <= max + 1; ++j) {
                if (temp < matches[j] - 1 && temp < m - 1) {
                    temp2 = matches[j];
                    matches[j] = temp + 1;
                    temp = temp2;
                    if (max < j) max = j;
                } else temp = matches[j];
            }
        }
        return (m + n) - max;
    }

    /**
     * Given a point, The subroutine finds points in that tile.
     * And for those points, subroutine finds the trajectory-position pair of all trajectories passing these points.
     *
     * @param nodeGridIndex gridIndex indexing all the nodes.
     * @param point query point.
     * @return a list of traj_id-position Pairs for trajectories near by that point.
     */
    public List<NodePair<Integer>> find(NodeGridIndex nodeGridIndex, Point point) {
        List<Integer> pointIDList = nodeGridIndex.find(point.getLat(), point.getLon()); //find points nearby
        List<NodePair<Integer>> res = new ArrayList<>();
        if (pointIDList != null) {
            for (Integer pointID : pointIDList) {
                List<NodePair<Integer>> candidate = find(pointID); // find trajectories passing that point.
                if (candidate != null)
                    res.addAll(candidate);
            }
        }
        return res;
    }

    /**
     * @param id point hash
     * @return
     */
    public List<NodePair<Integer>> find(int id) {
        if (loadCompressedIndex) {
            long startTime = System.nanoTime();
            List<NodePair<Integer>> res = new ArrayList<>();
            try {
                int[] compressedTrajIDArray = comIndexTrajID.get(id);
                int[] trajectoryIDArray = iic.uncompress(compressedTrajIDArray);
                byte[] compressedPositionArray = comIndexNodeTraPos.get(id);
                int[] positionArray = new int[trajectoryIDArray.length];
                bic.uncompress(compressedPositionArray, new IntWrapper(), compressedPositionArray.length, positionArray, new IntWrapper());
                for (int i = 0; i < positionArray.length; i++) {
                    res.add(new NodePair<>(trajectoryIDArray[i], positionArray[i]));
                }
            } catch (Exception e) {
                //logger.error("cannot find hash = {} from the dataStructure", hash);
            }
            compressionTime += (System.nanoTime() - startTime);
            return res;
        }
        return index.get(id);
    }

    /**
     * key for trajectory hash, value for position
     *
     * @param <T>
     */
    class NodePair<T extends Integer> implements Pair {

        NodePair(T key, T value) {
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

        final T key, value;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodePair<?> edgePair = (NodePair<?>) o;
            return Objects.equals(key, edgePair.key) &&
                    Objects.equals(value, edgePair.value);
        }

        @Override
        public int hashCode() {

            return Objects.hash(key, value);
        }
    }

}
