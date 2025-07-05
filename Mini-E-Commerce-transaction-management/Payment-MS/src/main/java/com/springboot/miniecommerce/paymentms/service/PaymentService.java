package com.springboot.miniecommerce.paymentms.service;

import com.springboot.miniecommerce.commonutils.constant.PaymentEnum;
import com.springboot.miniecommerce.commonutils.dto.ErrorDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.PaymentResponseDTO;
import com.springboot.miniecommerce.paymentms.model.Payment;
import com.springboot.miniecommerce.paymentms.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    public ResponseEntity<?> doPaymentForOrder(PaymentRequestDTO paymentRequestDTO) {
        if (paymentRequestDTO.getOrderId() == 0L) {
            log.error("Order ID is invalid, aborting payment");
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), PaymentEnum.ORDER_ID_NULL.toString(), "OrderId supplied is invalid", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.BAD_REQUEST);
        }

        if (paymentRequestDTO.getAmount() == 0D) {
            log.error("Amount is invalid, aborting payment");
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), PaymentEnum.PAYMENT_AMOUNT_NULL.toString(), "Amount supplied is invalid", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.BAD_REQUEST);
        }

        Payment payment = new Payment();
        payment.setOrderId(paymentRequestDTO.getOrderId());
        payment.setTotalAmount(paymentRequestDTO.getAmount());
        payment = paymentRepository.saveAndFlush(payment);
        log.info("Payment details saved successfully for orderId:{} and amount:{}", payment.getOrderId(), payment.getTotalAmount());

        PaymentResponseDTO paymentResponseDTO = new PaymentResponseDTO(payment.getPaymentId(), payment.getTotalAmount());
        return new ResponseEntity<>(paymentResponseDTO, HttpStatus.OK);
    }
}
