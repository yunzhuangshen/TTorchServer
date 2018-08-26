package au.edu.rmit.bdm.TTorchServer;

import au.edu.rmit.bdm.Torch.base.model.Coordinate;
import au.edu.rmit.bdm.Torch.base.model.TrajEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

class Converter {
    private static Logger logger = LoggerFactory.getLogger(Converter.class);

    /**
     * Try to model incoming string to a list of trajEntry.
     * If the string is in wrong format and can not be modeled,
     * null will be returned.
     */
    static List<TrajEntry> convertTrajectory(String coords){
        try {
            int len = coords.length();
            coords = coords.substring(2, len - 2); // remove leading "[[" and trailing "]]"
            String[] tuples = coords.split("],\\[");


            List<TrajEntry> entries = new ArrayList<>(tuples.length);
            String[] temp;
            double lat;
            double lng;
            for (String tuple : tuples) {
                temp = tuple.split(",");
                lat = Double.valueOf(temp[0]);
                lng = Double.valueOf(temp[1]);
                entries.add(new Coordinate(lat, lng));
            }
            return entries;

        }catch (Exception e){
            logger.warn("cannot model input queryType: '{}'!", coords);
            return null;
        }
    }

}
