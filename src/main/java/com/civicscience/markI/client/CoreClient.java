package com.civicscience.markI.client;


import com.civicscience.markI.client.dto.CheckboxGroup;
import com.civicscience.markI.client.dto.Question;
import com.civicscience.markI.client.dto.QuestionCheckboxItem;
import feign.RequestLine;
import org.springframework.cloud.openfeign.FeignClient;

import java.util.List;

@FeignClient(path = "")
public interface CoreClient {
    @RequestLine("GET /api/1/questions/{id}")
    Question getQuestion(String id);

    @RequestLine("GET /api/1/questions/{id}")
    CheckboxGroup getCheckboxGroup(String id);

    @RequestLine("GET /api/1/questions/{id}/checkbox_items")
    List<QuestionCheckboxItem> getQuestionCheckboxItem(String id);
}