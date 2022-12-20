package com.civicscience.markI.dto;

import com.civicscience.markI.constants.AskableType;
import com.civicscience.markI.constants.Operation;
import lombok.Getter;

import java.util.Optional;

@Getter
public abstract class AskableDTO {
    private String id;
    private Operation operation;
    private AskableType type;
    private Optional<AskableDataDTO> data;

}
