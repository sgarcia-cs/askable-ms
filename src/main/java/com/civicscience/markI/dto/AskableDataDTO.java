package com.civicscience.markI.dto;

import lombok.Getter;

import java.util.Date;

@Getter
public class AskableDataDTO {
    private String text;
    private String id;
    private RolesDTO roles;
    private String userId;
    private String accountId;
    private String topic;
    private Date created;
    private Date updated;
    private boolean is_sponsored;
    private String locale;
    private SharingDTO sharing;
}


