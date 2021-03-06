package com.cdr.gen;

import org.joda.time.Interval;

import java.util.Objects;
import java.util.UUID;

/**
 * Holds information about a call.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class Call {
    private UUID id;
    private Cell cell;
    private int line;
    private String type;
    private Interval time;
    private double cost;
    private String destPhoneNumber;
    private Fraud fraud = Fraud.NONE;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Interval getTime() {
        return time;
    }

    public void setTime(Interval time) {
        this.time = time;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public int getLine() {
        return line;
    }

    public void setLine(int line) {
        this.line = line;
    }

    public String getDestPhoneNumber() {
        return destPhoneNumber;
    }

    public void setDestPhoneNumber(String destPhoneNumber) {
        this.destPhoneNumber = destPhoneNumber;
    }

    public Cell getCell() {
        return cell;
    }

    public void setCell(Cell cell) {
        this.cell = cell;
    }

    public Fraud getFraud() {
        return fraud;
    }

    public void setFraud(Fraud fraud) {
        this.fraud = fraud;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Call call = (Call) o;
        return Objects.equals(id, call.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public Call copy() {
        Call c = new Call();
        c.id = id;
        c.cell = cell;
        c.line = line;
        c.type = type;
        c.time = time;
        c.cost = cost;
        c.destPhoneNumber = destPhoneNumber;
        c.fraud = fraud;
        return c;
    }

    public Call copyWithId(UUID id) {
        Call c = copy();
        c.id = id;
        return c;
    }
}
