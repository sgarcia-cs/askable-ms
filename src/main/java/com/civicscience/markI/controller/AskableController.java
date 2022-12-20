package com.civicscience.markI.controller;

import com.civicscience.markI.dto.AskableDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AskableController {
    @PostMapping()
    void setAskable(@RequestBody AskableDTO askable) {

    }
}
