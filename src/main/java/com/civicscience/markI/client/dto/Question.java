package com.civicscience.markI.client.dto;

import java.util.ArrayList;
import java.util.Date;

public class Question{
    public String qtext;
    public String qtype;
    public int survey_id;
    public String keyword;
    public int random;
    public int qorder;
    public String topic;
    public int ver;
    public int repeat_ineligible_seconds;
    public int parent_id;
    public boolean is_public;
    public String option_arrange_type;
    public String reporting_label;
    public String mime_type;
    public int account_refid;
    public boolean is_sponsored;
    public boolean has_role_engagement;
    public boolean has_role_value;
    public boolean has_role_profile;
    public boolean is_global_network_approved;
    public boolean is_deleted;
    public boolean has_customer_timeview;
    public boolean hide_results;
    public String explanation_text_title;
    public String weighting_scheme_name;
    public String sub_type;
    public int question_id;
    public Date updated;
    public int user_refid;
    public Date created_time;
    public User user;
    public Account account;
    public ArrayList<AnswerChoice> answer_choices;
    public ArrayList<String> permissions;
    public String deployment_status;
}


