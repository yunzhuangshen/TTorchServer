package au.edu.rmit.trajectory.similarity.persistence;

import au.edu.rmit.trajectory.similarity.model.MapMatchedTrajectory;
import au.edu.rmit.trajectory.similarity.model.Trajectory;
import org.apache.ibatis.annotations.Select;
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
public interface TrajectoryMapper {

    List<Trajectory> getAllTrajectories();

    Trajectory getTrajectory(int id);

    int insertTrajectory(Trajectory trajectory);

    int insertTrajectories(List<Trajectory> trajectories);

    List<Trajectory> getTrajectories(List<Integer> ids);

    int delTrajectories(List<Integer> ids);

    int delAllTrajectories();

    int updateMMEdges(List<Trajectory> trajectories);
}
