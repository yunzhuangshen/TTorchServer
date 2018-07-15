package au.edu.rmit.trajectory.similarity.service;

import au.edu.rmit.trajectory.similarity.model.MapMatchedTrajectory;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public interface MapMatchedTrajectoryService {

    List<MapMatchedTrajectory> getAllMapMatchedTrajectory();

    MapMatchedTrajectory getMapMatchedTrajectory(int id);

    int insertMapMatchedTrajectory(MapMatchedTrajectory mapMatchedTrajectory);

    int insertMapMatchedTrajectories(List<MapMatchedTrajectory> mapMatchedTrajectories);
}
