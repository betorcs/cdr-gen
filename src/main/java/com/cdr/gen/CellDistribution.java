package com.cdr.gen;

import com.cdr.gen.util.IOUtils;
import com.cdr.gen.util.JavaUtils;
import com.cdr.gen.util.RandomUtil;
import org.apache.log4j.Logger;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CellDistribution {

    private static final Logger LOG = Logger.getLogger(CellDistribution.class);
    private static final String CELL_DIST_CSV = "/cell_dist.csv";
    private static final List<Cell> CELLS = new ArrayList<>();

    static {
        try {
            LOG.info("Loading cell files.");
            ICsvListReader listReader;

            if (JavaUtils.isJar()) {
                InputStream is = CellDistribution.class.getResourceAsStream(CELL_DIST_CSV);
                listReader = new CsvListReader(
                        new StringReader(IOUtils.convertStreamToString(is)),
                        CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
            } else {
                listReader = new CsvListReader(
                        new FileReader("src/main/resources" + CELL_DIST_CSV),
                        CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
            }

            listReader.getHeader(true);

            List<String> cellList;
            while ((cellList = listReader.read()) != null) {
                String cellId = cellList.get(0);
                String latStr = cellList.get(1);
                String lonStr = cellList.get(2);

                CELLS.add(new Cell(
                        cellId,
                        Double.parseDouble(latStr),
                        Double.parseDouble(lonStr))
                );
            }

            listReader.close();
        } catch (FileNotFoundException e) {
            LOG.error("Unable to find cell file.", e);
        } catch (IOException e) {
            LOG.error("Error while reading the cell file.", e);
        }
    }

    /**
     * Returns a randomly picked cell;
     *
     * @return The cell
     */
    public Cell getRandomCell() {
        int position = RandomUtil.randInt(0, CELLS.size() - 1);
        return CELLS.get(position);
    }

    /**
     * Returns a randomly picked cell with {@param minDistanceInMeters} from given {@param cellId}
     *
     * @param cellId              Given cell ID
     * @param minDistanceInMeters Minimum distance
     * @return The cell
     */
    public Cell getRandomCell(String cellId, double minDistanceInMeters) {
        Cell cell = getCellById(cellId);

        return CELLS.stream()
                .filter(c -> c.distance(cell) >= minDistanceInMeters)
                .sorted((a, b) -> ThreadLocalRandom.current().nextInt(-1, 2))
                .findAny()
                .orElseThrow(() -> new RuntimeException("No cell distant " + minDistanceInMeters + "m of " + cellId + ""));
    }

    public Cell getCellById(String cellId) {
        return CELLS.stream()
                .filter(c -> c.getId().equals(cellId))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No cell found with ID \"" + cellId + "\""));
    }

}
