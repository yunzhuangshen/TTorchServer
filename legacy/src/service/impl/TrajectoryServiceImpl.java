package au.edu.rmit.trajectory.similarity.service.impl;

import au.edu.rmit.trajectory.similarity.model.Trajectory;
import au.edu.rmit.trajectory.similarity.persistence.PointMapper;
import au.edu.rmit.trajectory.similarity.persistence.TrajectoryMapper;
import au.edu.rmit.trajectory.similarity.service.TrajectoryService;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Service("TrajectoryService")
public class TrajectoryServiceImpl implements TrajectoryService {

    private static Logger logger = LoggerFactory.getLogger(TrajectoryServiceImpl.class);

    @Autowired
    private TrajectoryMapper trajectoryMapper;

    @Autowired
    private PointMapper pointMapper;

    @Override
    public List<Trajectory> getAllTrajectories() {
        return trajectoryMapper.getAllTrajectories();
    }

    @Override
    public List<Trajectory> getTrajectories(List<Integer> ids) {
        return trajectoryMapper.getTrajectories(ids);
    }

    @Override
    public int delTrajectories(List<Integer> ids) {
        return trajectoryMapper.delTrajectories(ids);
    }

    @Override
    public Trajectory getTrajectory(int id) {
        return trajectoryMapper.getTrajectory(id);
    }

    @Override
    public int insertTrajectories(List<Trajectory> trajectories) {
        return trajectoryMapper.insertTrajectories(trajectories);
    }

    @Override
    public int delAllTrajectories() {
        return trajectoryMapper.delAllTrajectories();
    }

    @Override
    public int updateMMEdges(List<Trajectory> trajectories) {
        return trajectoryMapper.updateMMEdges(trajectories);
    }

    @Override
    public int insertTrajectory(Trajectory trajectory) {
        return trajectoryMapper.insertTrajectory(trajectory);
    }
}
