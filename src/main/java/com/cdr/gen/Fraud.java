package com.cdr.gen;

public enum Fraud {

    FAR(1), UNUSUAL(2), NONE(0);

    private final int value;

    Fraud(int value) {
        this.value = value;
    }

    public int intValue() {
        return value;
    }

}
