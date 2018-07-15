package au.edu.rmit.trajectory.similarity.task;

import au.edu.rmit.trajectory.similarity.Common;
import au.edu.rmit.trajectory.similarity.task.formatter.LineFormatter;
import com.graphhopper.util.GPXEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static java.lang.Math.cos;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

/**
 * @author forrest0402
 * @Description
 * @date 11/16/2017
 */
@Component
@Scope("prototype")
public class TrajectoryConvertTask implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(TrajectoryConvertTask.class);

    final BlockingQueue<String> RAW_LINE_DATA;
    final String THREAD_NAME;
    final List<List<GPXEntry>> INPUT_GPX_ENTRIES;
    final LineFormatter FORMATTER;

    public TrajectoryConvertTask(BlockingQueue<String> rawLineData, String currentName, List<List<GPXEntry>> inputGPXEntries, LineFormatter formatter) {
        this.RAW_LINE_DATA = rawLineData;
        this.THREAD_NAME = currentName;
        this.INPUT_GPX_ENTRIES = inputGPXEntries;
        this.FORMATTER = formatter;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(THREAD_NAME);
        while (true) {
            try {
                String lineStr = this.RAW_LINE_DATA.take();
                //logger.info("take " + lineStr);
                if (Common.instance.STOP_CHARACTOR.equals(lineStr)) break;

                List<GPXEntry> trajectory = FORMATTER.format(lineStr);
                if (trajectory == null || trajectory.size() < 2) continue;
                synchronized (TrajectoryConvertTask.class) {
                    this.INPUT_GPX_ENTRIES.add(trajectory);
                }

            } catch (InterruptedException e) {
                logger.error(e.getLocalizedMessage());
            }
        }
    }
}
