package com.cdr.gen;

public class Cell {

    private String id;
    private double lat;
    private double lon;

    public Cell() {
    }

    public Cell(String id, double lat, double lon) {
        this.id = id;
        this.lat = lat;
        this.lon = lon;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double distance(Cell cell) {
        final int R = 6371; // Radius of the earth
        
        double latDistance = Math.toRadians(this.lat - cell.lat);
        double lonDistance = Math.toRadians(this.lon - cell.lon);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(cell.lat)) * Math.cos(Math.toRadians(this.lat))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c * 1000; // convert to meters

    }
}
