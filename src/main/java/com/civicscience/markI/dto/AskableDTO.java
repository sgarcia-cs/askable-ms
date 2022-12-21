package com.civicscience.markI.dto;

import com.civicscience.markI.constant.AskableType;
import com.civicscience.markI.constant.Operation;
import lombok.Getter;

import java.util.Optional;

@Getter
public abstract class AskableDTO {
    private String id;
    private Operation operation;
    private AskableType type;
    private Optional<AskableDataDTO> metaData;

}
