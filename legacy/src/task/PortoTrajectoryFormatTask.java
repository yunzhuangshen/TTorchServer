package au.edu.rmit.trajectory.similarity.task;

import au.edu.rmit.trajectory.similarity.Common;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author forrest0402
 * @Description
 * @date 11/16/2017
 */
@Component
@Scope("prototype")
public class PortoTrajectoryFormatTask implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PortoTrajectoryFormatTask.class);

    private static final int TIMESTAMP = 5;
    private static final int MISSING_DATA = 7;
    private static final int POLYLINE = 8;
    private String[] lineArray = new String[POLYLINE + 1];

    final BlockingQueue<String> RAW_LINE_DATA;
    final String THREAD_NAME;
    final List<String> TRAJ_POINTS;

    public PortoTrajectoryFormatTask(BlockingQueue<String> rawLineData, String currentName, List<String> inputGPXEntries) {
        this.RAW_LINE_DATA = rawLineData;
        this.THREAD_NAME = currentName;
        this.TRAJ_POINTS = inputGPXEntries;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(THREAD_NAME);
        while (true) {
            try {
                String lineStr = this.RAW_LINE_DATA.take();
                //logger.info("take " + lineStr);
                if (Common.instance.STOP_CHARACTOR.equals(lineStr)) break;

                Matcher matcher = Pattern.compile("\"(.*?)\"").matcher(lineStr);
                int idx = 0;
                while (matcher.find()) {
                    lineArray[idx++] = matcher.group(1);
                }
                if ("False".equals(lineArray[MISSING_DATA]) && !"[]".equals(lineArray[POLYLINE])) {
                    synchronized (PortoTrajectoryFormatTask.class) {
                        this.TRAJ_POINTS.add(lineArray[POLYLINE]);
                    }
                }

            } catch (InterruptedException e) {
                logger.error(e.getLocalizedMessage());
            }
        }
    }
}
