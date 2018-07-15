package au.edu.rmit.trajectory.similarity.service.impl;

import au.edu.rmit.trajectory.similarity.model.MMEdge;
import au.edu.rmit.trajectory.similarity.persistence.MMEdgeMapper;
import au.edu.rmit.trajectory.similarity.service.MMEdgeService;
import au.edu.rmit.trajectory.similarity.service.MMEdgeService;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 11/23/2017
 */
@Service("MMEdgeServiceImpl")
public class MMEdgeServiceImpl implements MMEdgeService {

    private static Logger logger = LoggerFactory.getLogger(MMEdgeServiceImpl.class);

    @Autowired
    MMEdgeMapper segmentMapper;

    @Override
    public int deleteMMEdges(List<MMEdge> segments) {
        return segmentMapper.insertMMEdges(segments);
    }

    @Override
    public int deleteAllMMEdges() {
        return segmentMapper.deleteAllMMEdges();
    }

    @Override
    public int insertMMEdge(MMEdge segment) {
        return segmentMapper.insertMMEdge(segment);
    }

    @Override
    public int insertMMEdges(List<MMEdge> segments) {
        return segmentMapper.insertMMEdges(segments);
    }

    @Override
    public MMEdge getMMEdge(@Param("hash") int id) {
        return segmentMapper.getMMEdge(id);
    }

    @Override
    public List<MMEdge> getAllMMEdges() {
        List<MMEdge> edges = segmentMapper.getAllMMEdges();
        for (MMEdge edge : edges) {
            edge.convertFromDatabaseForm();
        }
        return edges;
    }

    @Override
    public Map<Integer, MMEdge> getAllEdges() {
        List<MMEdge> edges = segmentMapper.getAllMMEdges();
        Map<Integer, MMEdge> dataset = new HashMap<>();
        for (MMEdge edge : edges) {
            edge.convertFromDatabaseForm();
            dataset.put(edge.getId(),edge);
        }
        edges.clear();
        return dataset;
    }
}
