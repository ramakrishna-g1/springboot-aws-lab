package com.springboot.miniecommerce.paymentms.controller;

import com.springboot.miniecommerce.commonutils.dto.ApiResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentRequestDTO;
import com.springboot.miniecommerce.paymentms.service.PaymentService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/payment")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService){
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<ApiResponseDTO<?>> doPaymentForOrder(@RequestBody PaymentRequestDTO paymentRequestDTO){
        return paymentService.doPaymentForOrder(paymentRequestDTO);
    }
}
