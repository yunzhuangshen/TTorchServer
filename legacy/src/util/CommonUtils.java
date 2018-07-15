package au.edu.rmit.trajectory.similarity.util;

import com.javamex.classmexer.MemoryUtil;
import me.lemire.integercompression.ByteIntegerCODEC;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author forrest0402
 * @Description
 * @date 1/23/2018
 */
public class CommonUtils {

    private static Logger logger = LoggerFactory.getLogger(CommonUtils.class);

    final static String SEPRATOR = ";";

    /**
     * compress indexes files.
     *
     * compression techniques:
     * 1. IntegratedIntCompressor -- Delta Encoding: leverage gap information.
     *    use case: In this example it is used for list of trajIds.
     *
     * 2. ByteIntegerCODEC -- VByte: using less bytes to store integers.
     *    use case: In this example it is used for list of pos corresponding to trajIds.
     *
     * @param INDEX_FILE1_TRA_ID_REORDER_COMPRESSED path of output file for tra_id reordering compression.
     * @param INDEX_FILE_TRA_ID_COMPRESSED  compressed trajectory file without tra_id reordering.
     * @param INDEX_FILE_TRA_ID    trajectory file
     * @param INDEX_FILE_POS    position file
     * @param INDEX_FILE_POS_COMPRESSED  compressed position file
     * @param INDEX_FILE_NODE_ID  hash file
     * @param reorder if true, INDEX_FILE1_TRA_ID_REORDER_COMPRESSED will be used for the compressed tra_ids output file.
     *                Before compression, tra_ids are reordered according to key-value in beijing-bisected.mapping.
     *                if false, INDEX_FILE_TRA_ID_COMPRESSED will be used for the compressed tra_ids output file.
     */
    //todo pos list should be reordered while trajId list reorderd.
    public static void compress(String INDEX_FILE1_TRA_ID_REORDER_COMPRESSED, String INDEX_FILE_TRA_ID_COMPRESSED, String INDEX_FILE_TRA_ID, String INDEX_FILE_POS, String INDEX_FILE_POS_COMPRESSED, String INDEX_FILE_NODE_ID, boolean reorder) {
        logger.info("Enter compress");
        String outputStr = reorder ? INDEX_FILE1_TRA_ID_REORDER_COMPRESSED : INDEX_FILE_TRA_ID_COMPRESSED;
        Map<Integer, Integer> reorderMap = new HashMap<>();
        try (//BufferedReader idBufReader = new BufferedReader(new FileReader(INDEX_FILE_NODE_ID));
             BufferedReader reorderBufReader = new BufferedReader(new FileReader("beijing-bisected.mapping"));
             BufferedReader trajIdReader = new BufferedReader(new FileReader(INDEX_FILE_TRA_ID));
             BufferedWriter trajBufWriter = new BufferedWriter((new OutputStreamWriter(new FileOutputStream(outputStr, false), StandardCharsets.UTF_8)))) {


            String trajLine, idString;
            while ((trajLine = reorderBufReader.readLine()) != null) {
                String[] array = trajLine.split(" ");
                reorderMap.put(Integer.parseInt(array[0]), Integer.parseInt(array[1]));
            }

            IntegratedIntCompressor iic = new IntegratedIntCompressor();
            while ((trajLine = trajIdReader.readLine()) != null) {
                String[] trajArray = trajLine.split(SEPRATOR);
                int[] data = new int[trajArray.length];
                for (int i = 0; i < trajArray.length; i++) {
                    String str = trajArray[i];
                    data[i] = Integer.parseInt(str);
                    if (reorder)
                        data[i] = reorderMap.get(data[i]);
                }
                Arrays.sort(data);
                int[] compressed = iic.compress(data);
                boolean first = true;
                for (int i : compressed) {
                    if (first)
                        first = false;
                    else trajBufWriter.write(SEPRATOR);
                    trajBufWriter.write(String.valueOf(i));
                }
                trajBufWriter.newLine();
            }
            logger.info("load complete");
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedReader posBufReader = new BufferedReader(new FileReader(INDEX_FILE_POS));
             OutputStream posBufWriter = new FileOutputStream(INDEX_FILE_POS_COMPRESSED, false)) {
            String posLine;
            ByteIntegerCODEC bic = new VariableByte();
            byte[] SEPBYTE = new byte[]{0x1F, 0x3F, 0x5F, 0x7F};
            while ((posLine = posBufReader.readLine()) != null) {
                String[] posArray = posLine.split(SEPRATOR);
                int[] data = new int[posArray.length];
                for (int i = 0; i < posArray.length; i++) {
                    data[i] = Integer.parseInt(posArray[i]);
                }
                byte[] outArray = new byte[1000000];
                IntWrapper inPos = new IntWrapper(), outPos = new IntWrapper();
                bic.compress(data, inPos, data.length, outArray, outPos);
                posBufWriter.write(outArray, 0, outPos.get());
                posBufWriter.write(SEPBYTE);
            }
            logger.info("load complete");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Exit compress");
    }

    public static void printObjectSize(Object object) {
        long numOfBytes = MemoryUtil.deepMemoryUsageOf(object, MemoryUtil.VisibilityFilter.ALL);
        //System.out.println(numOfBytes / (1024 * 1024.0) + " MB");
        System.out.println(numOfBytes / (1024.0) + " MB");
    }

    public static int getNumberOfCPUCores() {
        String command = "";
        if (OSValidator.isMac()) {
            command = "sysctl -n machdep.cpu.core_count";
        } else if (OSValidator.isUnix()) {
            command = "lscpu";
        } else if (OSValidator.isWindows()) {
            command = "cmd /C WMIC CPU Get /Format:List";
        }
        Process process = null;
        int numberOfCores = 0;
        int sockets = 0;
        try {
            if (OSValidator.isMac()) {
                String[] cmd = {"/bin/sh", "-c", command};
                process = Runtime.getRuntime().exec(cmd);
            } else {
                process = Runtime.getRuntime().exec(command);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
        String line;

        try {
            while ((line = reader.readLine()) != null) {
                if (OSValidator.isMac()) {
                    numberOfCores = line.length() > 0 ? Integer.parseInt(line) : 0;
                } else if (OSValidator.isUnix()) {
                    if (line.contains("Core(s) per socket:")) {
                        numberOfCores = Integer.parseInt(line.split("\\s+")[line.split("\\s+").length - 1]);
                    }
                    if (line.contains("Socket(s):")) {
                        sockets = Integer.parseInt(line.split("\\s+")[line.split("\\s+").length - 1]);
                    }
                } else if (OSValidator.isWindows()) {
                    //NumberOfLogicalProcessors NumberOfCores
                    if (line.contains("NumberOfLogicalProcessors")) {
                        numberOfCores = Integer.parseInt(line.split("=")[1]);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (OSValidator.isUnix()) {
            return numberOfCores * sockets;
        }
        return numberOfCores;
    }
}
class OSValidator {

    private static String OS = System.getProperty("os.name").toLowerCase();

    public static void main(String[] args) {

        System.out.println(OS);

        if (isWindows()) {
            System.out.println("This is Windows");
        } else if (isMac()) {
            System.out.println("This is Mac");
        } else if (isUnix()) {
            System.out.println("This is Unix or Linux");
        } else if (isSolaris()) {
            System.out.println("This is Solaris");
        } else {
            System.out.println("Your OS is not support!!");
        }
    }

    public static boolean isWindows() {
        return (OS.contains("win"));
    }

    public static boolean isMac() {
        return (OS.contains("mac"));
    }

    public static boolean isUnix() {
        return (OS.contains("nix") || OS.contains("nux") || OS.indexOf("aix") > 0);
    }

    public static boolean isSolaris() {
        return (OS.contains("sunos"));
    }

    public static String getOS() {
        if (isWindows()) {
            return "win";
        } else if (isMac()) {
            return "osx";
        } else if (isUnix()) {
            return "uni";
        } else if (isSolaris()) {
            return "sol";
        } else {
            return "err";
        }
    }
}