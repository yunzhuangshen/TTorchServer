package au.edu.rmit.trajectory.similarity.persistence;

import au.edu.rmit.trajectory.similarity.model.MapMatchedTrajectory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public interface MapMatchedTrajectoryMapper {

    List<MapMatchedTrajectory> getAllMapMatchedTrajectory();

    MapMatchedTrajectory getMapMatchedTrajectory(int id);

    int insertMapMatchedTrajectory(MapMatchedTrajectory mapMatchedTrajectory);

    int insertMapMatchedTrajectories(List<MapMatchedTrajectory> mapMatchedTrajectories);
}
