package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;

import java.util.LinkedList;
import java.util.List;

class ResultObj {
    boolean mappingSucceed;
    String failReason;
    List<Coordinate> mappedTrajectory;
    List<Coordinate> rawTrajectory;
    int retSize;
    int[] ids;

    ResultObj(){
        mappedTrajectory = new LinkedList<>();
        rawTrajectory = new LinkedList<>();
    }
}
