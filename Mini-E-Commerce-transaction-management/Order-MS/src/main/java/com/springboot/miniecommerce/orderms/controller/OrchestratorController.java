package com.springboot.miniecommerce.orderms.controller;

import com.springboot.miniecommerce.commonutils.dto.ApiResponseDTO;
import com.springboot.miniecommerce.orderms.model.Order;
import com.springboot.miniecommerce.orderms.service.OrchestrateOrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orchestrator")
public class OrchestratorController {

    private final OrchestrateOrderService orchestrateOrderService;

    public OrchestratorController(OrchestrateOrderService orchestrateOrderService) {
        this.orchestrateOrderService = orchestrateOrderService;
    }

    @PostMapping("/order")
    public ResponseEntity<ApiResponseDTO<?>> createOrder(@RequestBody Order order) {
        //create order and send response
        return orchestrateOrderService.orchestrateOrderCreation(order);
    }
}
