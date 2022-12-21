package com.civicscience.markI.client.dto;

import java.util.ArrayList;
import java.util.Date;

public class CheckboxGroup{
    public String group_text;
    public String question_type;
    public boolean random;
    public String none_text;
    public int checks_min;
    public int checks_max;
    public User user;
    public Date created;
    public ArrayList<CheckboxItem> checkbox_items;
}