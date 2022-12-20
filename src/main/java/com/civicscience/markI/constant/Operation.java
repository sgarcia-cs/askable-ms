package com.civicscience.markI.constant;

import lombok.Getter;

@Getter
public enum Operation {
    INSERT_OPERATION ("I"),
    UPDATE_OPERATION ("U"),
    DELETE_OPERATION ("D");

    private String value;
    Operation(String op) {
        this.value = op;
    }
}