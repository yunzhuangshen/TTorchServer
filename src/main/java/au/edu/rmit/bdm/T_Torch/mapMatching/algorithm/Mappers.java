package au.edu.rmit.bdm.T_Torch.mapMatching.algorithm;

import au.edu.rmit.bdm.T_Torch.base.Torch;
import com.graphhopper.routing.AlgorithmOptions;
import com.graphhopper.routing.weighting.FastestWeighting;
import com.graphhopper.util.Parameters;

/**
 * An Factory class provides simple APIs for instantiating map-matching algorithms supported by T-Torch.
 */
public abstract class Mappers {


    /**
     * Create specified map-matching algorithm for algorithm raw bdm.
     *
     * @param algorithm Specify which map-matching algorithm should be used to do bdm projection.
     * @param graph Graph used to project bdm on.
     * @return The map-matching algorithm.
     * @see Torch.Algorithms Valid options for first param.
     */
    public static Mapper getMapper(String algorithm, TorGraph graph) {

        if (graph == null)
            throw new IllegalStateException("cannot do map-matching without a graph");

        Mapper mapper;

        switch (algorithm) {
            case Torch.Algorithms.HMM:
                AlgorithmOptions algorithmOptions = AlgorithmOptions.start().
                        algorithm(Parameters.Algorithms.DIJKSTRA).weighting(new FastestWeighting(graph.vehicle)).build();
                mapper = new HiddenMarkovModel(graph, algorithmOptions);
                break;
            case Torch.Algorithms.HMM_PRECOMPUTED:
                mapper = new PrecomputedHiddenMarkovModel(graph);
                break;
            default:
                throw new IllegalStateException("lookup Torch.Algorithms for vehicle options");
        }

        return mapper;
    }

}
