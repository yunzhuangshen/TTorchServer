package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.model.Edge;
import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.model.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author forrest0402
 * @Description
 * @date 11/14/2017
 */
@Component
public class LongestCommonSubsequence {

    private static Logger logger = LoggerFactory.getLogger(LongestCommonSubsequence.class);

    /**
     * @param T1
     * @param T2
     * @param theta
     * @return
     */
    public double run(List<Segment> T1, List<Segment> T2, int theta) {

        if (T1 == null || T2 == null || T1.size() == 0 || T2.size() == 0)
            return 0;

        double[][] dpInts = new double[T1.size()][T2.size()];
        if (T1.get(0).equals(T2.get(0))) dpInts[0][0] = T1.get(0).getLength();
        for (int i = 1; i < T1.size(); ++i) {
            if (T1.get(i).equals(T2.get(0)))
                dpInts[i][0] = T1.get(i).getLength();
            else dpInts[i][0] = dpInts[i - 1][0];
        }

        for (int j = 1; j < T2.size(); ++j) {
            if (T2.get(j).equals(T1.get(0)))
                dpInts[0][j] = T2.get(j).getLength();
            else dpInts[0][j] = dpInts[0][j - 1];
        }

        for (int i = 1; i < T1.size(); ++i) {
            for (int j = 1; j < T2.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T1.get(i).equals(T2.get(j))) {
                        dpInts[i][j] = T1.get(i).getLength() + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                }
            }
        }

        return dpInts[T1.size() - 1][T2.size() - 1];
    }

    /**
     * @param T1
     * @param T2
     * @param theta
     * @return
     */
    public double mmRun(List<MMEdge> T1, List<MMEdge> T2, int theta) {

        if (T1 == null || T2 == null || T1.size() == 0 || T2.size() == 0)
            return 0;

        double[][] dpInts = new double[T1.size()][T2.size()];
        double len1 = T1.get(0).getLength(), len2 = T2.get(0).getLength();

        if (T1.get(0).getId() == T2.get(0).getId()) dpInts[0][0] = T1.get(0).getLength();
        for (int i = 1; i < T1.size(); ++i) {
            if (T1.get(i).getId() == T2.get(0).getId())
                dpInts[i][0] = T1.get(i).getLength();
            else dpInts[i][0] = dpInts[i - 1][0];
            len1 += T1.get(i).getLength();
        }

        for (int j = 1; j < T2.size(); ++j) {
            if (T2.get(j).getId() == T1.get(0).getId())
                dpInts[0][j] = T2.get(j).getLength();
            else dpInts[0][j] = dpInts[0][j - 1];
            len2 += T2.get(j).getLength();
        }

        for (int i = 1; i < T1.size(); ++i) {
            for (int j = 1; j < T2.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T1.get(i).getId() == T2.get(j).getId()) {
                        dpInts[i][j] = T1.get(i).getLength() + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                }
            }
        }

        return dpInts[T1.size() - 1][T2.size() - 1];// / Math.max(len1, len2);
    }

    /**
     * @param T1
     * @param T2
     * @param theta
     * @return
     */
    public double fastRun(List<Edge> T1, List<Edge> T2, int theta, double[] restDistance, double bestSofar) {

        if (T1 == null || T2 == null || T1.size() == 0 || T2.size() == 0)
            return 0;

        double[][] dpInts = new double[T1.size()][T2.size()];
        double len1 = T1.get(0).getLength(), len2 = T2.get(0).getLength();

        if (T1.get(0).getId() == T2.get(0).getId()) dpInts[0][0] = T1.get(0).getLength();
        for (int i = 1; i < T1.size(); ++i) {
            if (T1.get(i).getId() == T2.get(0).getId())
                dpInts[i][0] = T1.get(i).getLength();
            else dpInts[i][0] = dpInts[i - 1][0];
            len1 += T1.get(i).getLength();
        }

        for (int j = 1; j < T2.size(); ++j) {
            if (T2.get(j).getId() == T1.get(0).getId())
                dpInts[0][j] = T2.get(j).getLength();
            else dpInts[0][j] = dpInts[0][j - 1];
            len2 += T2.get(j).getLength();
        }

        for (int i = 1; i < T1.size(); ++i) {
            for (int j = 1; j < T2.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T1.get(i).getId() == T2.get(j).getId()) {
                        dpInts[i][j] = T1.get(i).getLength() + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                    if (dpInts[i][j] + restDistance[i] < bestSofar)
                        return dpInts[i][j] + restDistance[i];
                }
            }
        }

        return dpInts[T1.size() - 1][T2.size() - 1];/// Math.max(len1, len2);
    }

    /**
     *
     * @param T_query edges representing query trajectory
     * @param T_candidate edges representing sub candidate trajectory
     * @param theta
     @param restDistance a list containing the sum of rest edges length in total
      *                  example:
      *                  if the query contains 3 edges, which are 3 meters, 1 meters and 2 meters respectively in length.
      *                  Then the restDistance contains [3, 2, 0], which means that if it get to dataStructure 0, then the rest is 3.
      *                  If it get to dataStructure 1, then the rest is 2. And if it get to dataStructure 3, then the rest is 0.
     * @param bestKthSofar score for the min score element in the heap.
     * @param fullyScannNumber
     * @return similarity score computed using LORS sim measure.
     */
    public double fastRun(List<? extends Edge> T_query, List<? extends Edge> T_candidate, int theta, double[] restDistance, double bestKthSofar, AtomicInteger fullyScannNumber) {

        if (T_query == null || T_candidate == null || T_query.size() == 0 || T_candidate.size() == 0)
            return 0;

        double[][] dpInts = new double[T_query.size()][T_candidate.size()];
        double len1 = T_query.get(0).getLength(), len2 = T_candidate.get(0).getLength();

        if (T_query.get(0).getId() == T_candidate.get(0).getId()) dpInts[0][0] = T_query.get(0).getLength();

        for (int i = 1; i < T_query.size(); ++i) {
            if (T_query.get(i).getId() == T_candidate.get(0).getId())
                dpInts[i][0] = T_query.get(i).getLength();
            else dpInts[i][0] = dpInts[i - 1][0];
            len1 += T_query.get(i).getLength();
        }

        for (int j = 1; j < T_candidate.size(); ++j) {
            if (T_candidate.get(j).getId() == T_query.get(0).getId())
                dpInts[0][j] = T_candidate.get(j).getLength();
            else dpInts[0][j] = dpInts[0][j - 1];
            len2 += T_candidate.get(j).getLength();
        }

        for (int i = 1; i < T_query.size(); ++i) {
            for (int j = 1; j < T_candidate.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T_query.get(i).getId() == T_candidate.get(j).getId()) {
                        dpInts[i][j] = T_query.get(i).getLength() + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                    //todo
                    if (restDistance != null && dpInts[i][j] + restDistance[i] < bestKthSofar)
                        return dpInts[i][j] + restDistance[i];
                }
            }
        }

        return dpInts[T_query.size() - 1][T_candidate.size() - 1];
        /// Math.max(len1, len2);
    }

    /**
     * @param T1
     * @param T2
     * @param theta
     * @return
     */
    public double mmFastRun(List<MMEdge> T1, List<MMEdge> T2, int theta, double[] restDistance, double bestSofar) {

        if (T1 == null || T2 == null || T1.size() == 0 || T2.size() == 0)
            return 0;

        double[][] dpInts = new double[T1.size()][T2.size()];
        double len1 = T1.get(0).getLength(), len2 = T2.get(0).getLength();

        if (T1.get(0).getId() == T2.get(0).getId()) dpInts[0][0] = T1.get(0).getLength();
        for (int i = 1; i < T1.size(); ++i) {
            if (T1.get(i).getId() == T2.get(0).getId())
                dpInts[i][0] = T1.get(i).getLength();
            else dpInts[i][0] = dpInts[i - 1][0];
            len1 += T1.get(i).getLength();
        }

        for (int j = 1; j < T2.size(); ++j) {
            if (T2.get(j).getId() == T1.get(0).getId())
                dpInts[0][j] = T2.get(j).getLength();
            else dpInts[0][j] = dpInts[0][j - 1];
            len2 += T2.get(j).getLength();
        }

        for (int i = 1; i < T1.size(); ++i) {
            for (int j = 1; j < T2.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T1.get(i).getId() == T2.get(j).getId()) {
                        dpInts[i][j] = T1.get(i).getLength() + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                    if (dpInts[i][j] + restDistance[i] < bestSofar)
                        return dpInts[i][j] + restDistance[i];
                }
            }
        }

        return dpInts[T1.size() - 1][T2.size() - 1] / Math.max(len1, len2);
    }

    /**
     * @param T1
     * @param T2
     * @param theta
     * @return
     */
    public int testRun(List<?> T1, List<?> T2, int theta) {

        if (T1 == null || T2 == null || T1.size() == 0 || T2.size() == 0)
            return 0;

        int[][] dpInts = new int[T1.size()][T2.size()];
        if (T1.get(0).equals(T2.get(0))) dpInts[0][0] = 1;
        for (int i = 1; i < T1.size(); ++i) {
            if (T1.get(i).equals(T2.get(0)))
                dpInts[i][0] = 1;
            else dpInts[i][0] = dpInts[i - 1][0];
        }

        for (int j = 1; j < T2.size(); ++j) {
            if (T2.get(j).equals(T1.get(0)))
                dpInts[0][j] = 1;
            else dpInts[0][j] = dpInts[0][j - 1];
        }

        for (int i = 1; i < T1.size(); ++i) {
            for (int j = 1; j < T2.size(); ++j) {
                if (Math.abs(i - j) <= theta) {
                    if (T1.get(i).equals(T2.get(j))) {
                        dpInts[i][j] = 1 + dpInts[i - 1][j - 1];
                    } else {
                        dpInts[i][j] = Math.max(dpInts[i - 1][j], dpInts[i][j - 1]);
                    }
                }
            }
        }

        return dpInts[T1.size() - 1][T2.size() - 1];
    }
}
