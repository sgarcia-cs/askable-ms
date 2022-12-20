package com.civicscience.markI.constants;

import lombok.Getter;

@Getter
public enum AskableType {
    CHECKBOX_GROUP ("checkbox_group"),
    CHECKBOX_ITEM ("checkbox_item"),
    QUESTION ("question"),
    ANSWER ("answer");

    private String value;
    AskableType(String val) {
        this.value = val;
    }
}