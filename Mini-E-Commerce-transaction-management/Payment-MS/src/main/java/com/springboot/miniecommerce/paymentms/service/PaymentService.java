package com.springboot.miniecommerce.paymentms.service;

import com.springboot.miniecommerce.commonutils.constant.PaymentEnum;
import com.springboot.miniecommerce.commonutils.dto.ErrorDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentResponseDTO;
import com.springboot.miniecommerce.paymentms.model.Payment;
import com.springboot.miniecommerce.paymentms.repository.PaymentRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public ResponseEntity<?> doPaymentForOrder(PaymentRequestDTO paymentRequestDTO) {
        if (paymentRequestDTO.getOrderId() == null || paymentRequestDTO.getAmount() == null || paymentRequestDTO.getOrderId() == 0D) {
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), PaymentEnum.FAILED.toString(), "OrderId or Amount is null", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.BAD_REQUEST);
        }

        Payment payment = new Payment();
        payment.setOrderId(paymentRequestDTO.getOrderId());
        payment.setTotalAmount(paymentRequestDTO.getAmount());
        payment = paymentRepository.saveAndFlush(payment);

        PaymentResponseDTO paymentResponseDTO = new PaymentResponseDTO(payment.getPaymentId(), payment.getTotalAmount());
        return new ResponseEntity<>(paymentResponseDTO, HttpStatus.OK);
    }
}
