package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.TTorch.base.model.Coordinate;

import java.util.LinkedList;
import java.util.List;

class ResultObj {
    boolean mappingSucceed;
    List<Coordinate> mappedTrajectory;
    int retSize;
    int[] ids;

    ResultObj(){
        mappedTrajectory = new LinkedList<>();
    }
}
