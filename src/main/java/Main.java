import drivers.CountReverseSortDriver;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
    public static void main(String[] args) throws Exception {
        final Logger LOGGER = Logger.getLogger(Main.class);
        LOGGER.log(Level.INFO, "Program Started!");
        int res = ToolRunner.run(new CountReverseSortDriver(), args);
        System.exit(res);
    }
}