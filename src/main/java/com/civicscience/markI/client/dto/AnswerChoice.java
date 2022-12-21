package com.civicscience.markI.client.dto;

import java.util.Date;

public class AnswerChoice {
    public int question_id;
    public String answer_choice;
    public int aorder;
    public boolean allows_write_in;
    public int answer_choice_id;
    public Date updated;
    public QuizScore quiz_score;
}
