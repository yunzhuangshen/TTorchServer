package au.edu.rmit.trajectory.similarity.persistence;

import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Component
public interface PointMapper {

    int insertPoint(GPXEntry gpxEntry);

    GPXEntry getPoint(int id);
}
