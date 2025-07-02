package com.springboot.miniecommerce.paymentms.service;

import com.springboot.miniecommerce.commonutils.constant.PaymentStatus;
import com.springboot.miniecommerce.commonutils.dto.ApiResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentResponseDTO;
import com.springboot.miniecommerce.paymentms.model.Payment;
import com.springboot.miniecommerce.paymentms.repository.PaymentRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public ResponseEntity<ApiResponseDTO<?>> doPaymentForOrder(PaymentRequestDTO paymentRequestDTO) {
        if (paymentRequestDTO.getOrderId() == null || paymentRequestDTO.getAmount() == null || paymentRequestDTO.getOrderId() == 0D) {
            ApiResponseDTO<Void> apiResponseDTO = new ApiResponseDTO<>(PaymentStatus.FAILED.toString(), "OrderId or Amount is null");
            return new ResponseEntity<>(apiResponseDTO, HttpStatus.BAD_REQUEST);
        }

        Payment payment = new Payment();
        payment.setOrderId(paymentRequestDTO.getOrderId());
        payment.setTotalAmount(paymentRequestDTO.getAmount());
        payment = paymentRepository.saveAndFlush(payment);

        PaymentResponseDTO paymentResponseDTO= new PaymentResponseDTO(payment.getPaymentId(), payment.getTotalAmount());
        ApiResponseDTO<PaymentResponseDTO> apiResponseDTO = new ApiResponseDTO<>(PaymentStatus.SUCCESS.toString(), "Payment completed successfully", paymentResponseDTO);
        return new ResponseEntity<>(apiResponseDTO, HttpStatus.OK);
    }
}
