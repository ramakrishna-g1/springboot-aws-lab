package com.spring.aws_s3.dto;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class ResponseDTO {
    @SerializedName("Items")
    @Expose
    private List<StreamFileDTO> items;

    public ResponseDTO(List<StreamFileDTO> items) {
        this.items = items;
    }

    public List<StreamFileDTO> getItems() {
        return items;
    }

}
