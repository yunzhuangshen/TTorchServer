package au.edu.rmit.trajectory.similarity.algorithm;

import au.edu.rmit.trajectory.similarity.model.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Known as EDR algorithm, see "Robust and Fast Similarity Search for Moving Object Trajectories" for detail
 *
 * @author forrest0402
 * @Description
 * @date 11/15/2017
 */
@Component
public class EditDistanceonRealSequence {

    private static Logger logger = LoggerFactory.getLogger(EditDistanceonRealSequence.class);

    /**
     * Return the edit distance (insert, delete, or replace) between T1 and T2
     *
     * @param T1
     * @param T2
     * @return
     */
    public double run(List<Segment> T1, List<Segment> T2) {

        if (T1 == null || T1.size() == 0) {
            if (T2 != null) return T2.size();
            else return 0;
        }

        if (T2 == null || T2.size() == 0) {
            if (T1 != null) return T1.size();
            else return 0;
        }

        double[][] dpInts = new double[T1.size() + 1][T2.size() + 1];

        for (int i = 1; i <= T1.size(); ++i) {
            dpInts[i][0] = dpInts[i - 1][0] + T1.get(i - 1).getLength();
        }

        for (int j = 1; j <= T2.size(); ++j) {
            dpInts[0][j] = dpInts[0][j - 1] + T2.get(j - 1).getLength();
        }

        for (int i = 1; i <= T1.size(); ++i) {
            for (int j = 1; j <= T2.size(); ++j) {
                double subCost = 0;
                if (!T1.get(i - 1).equals(T2.get(j - 1)))
                    subCost = distance(T1.get(i - 1), T2.get(j - 1));
                dpInts[i][j] = min3(dpInts[i - 1][j - 1] + subCost, dpInts[i - 1][j] + T1.get(i - 1).getLength(), dpInts[i][j - 1] + T2.get(j - 1).getLength());
            }
        }

        return dpInts[T1.size()][T2.size()];
    }

    private double distance(Segment s1, Segment s2) {
        return s1.distanceTo(s2);
    }

    /**
     * Return the edit distance (insert, delete, or replace) between T1 and T2
     *
     * @param T1
     * @param T2
     * @return
     */
    public int testRun(List<?> T1, List<?> T2) {
        if (T1 == null || T1.size() == 0) {
            if (T2 != null) return T2.size();
            else return 0;
        }
        if (T2 == null || T2.size() == 0) {
            if (T1 != null) return T1.size();
            else return 0;
        }

        int[][] dpInts = new int[T1.size() + 1][T2.size() + 1];

        for (int i = 1; i <= T1.size(); ++i) {
            dpInts[i][0] = i;
        }

        for (int j = 1; j <= T2.size(); ++j) {
            dpInts[0][j] = j;
        }

        for (int i = 1; i <= T1.size(); ++i) {
            for (int j = 1; j <= T2.size(); ++j) {
                int subCost = 1;
                if (T1.get(i - 1).equals(T2.get(j - 1)))
                    subCost = 0;
                dpInts[i][j] = min3(dpInts[i - 1][j - 1] + subCost, dpInts[i - 1][j] + 1, dpInts[i][j - 1] + 1);
            }
        }

        return dpInts[T1.size()][T2.size()];
    }

    /**
     * Return the minimal value of a, b, c
     *
     * @param a
     * @param b
     * @param c
     * @return
     */
    private int min3(int a, int b, int c) {
        if (a > b) a = b;
        if (a > c) a = c;
        return a;
    }

    private double min3(double a, double b, double c) {
        if (a > b) a = b;
        if (a > c) a = c;
        return a;
    }
}
