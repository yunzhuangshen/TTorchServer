package au.edu.rmit.trajectory.similarity.service;

import au.edu.rmit.trajectory.similarity.model.Trajectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Service
public interface TrajectoryService {

    List<Trajectory> getAllTrajectories();

    List<Trajectory> getTrajectories(List<Integer> ids);

    int delTrajectories(List<Integer> ids);

    Trajectory getTrajectory(int id);

    int insertTrajectory(Trajectory trajectory);

    int insertTrajectories(List<Trajectory> trajectories);

    int delAllTrajectories();

    int updateMMEdges(List<Trajectory> trajectories);
}
