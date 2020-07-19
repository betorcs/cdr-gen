package com.cdr.gen;

import junit.framework.TestCase;

public class CellDistributionTest extends TestCase {

    private CellDistribution cellDistribution;

    public CellDistributionTest(String testName) {
        super(testName);

        this.cellDistribution = new CellDistribution();
    }

    public void testGetRandomCell() {
        Cell cell = cellDistribution.getRandomCell();

        assertNotNull(cell);
    }

    public void testGetRandomCellWithDistance() {
        Cell cell = cellDistribution.getCellById("Cell_0");

        double distance = 4511469;

        Cell randomCell = cellDistribution.getRandomCell(cell.getId(), distance);

        assertNotNull(randomCell);
        assertTrue(cell.distance(randomCell) >= distance);
        assertEquals("Cell_20", randomCell.getId());
    }
}
