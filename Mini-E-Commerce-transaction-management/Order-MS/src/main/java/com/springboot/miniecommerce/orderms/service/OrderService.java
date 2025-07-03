package com.springboot.miniecommerce.orderms.service;

import com.springboot.miniecommerce.commonutils.constant.OrderStatus;
import com.springboot.miniecommerce.commonutils.dto.ErrorDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderUpdateRequestDTO;
import com.springboot.miniecommerce.orderms.model.Order;
import com.springboot.miniecommerce.orderms.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class OrderService {


    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public ResponseEntity<?> createOrder(Order order) {
        if (order.getProductId() == null || order.getProductId() == 0) {
            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderStatus.FAILED.toString(), "Order creation failed, product Id is null", LocalDateTime.now().toString());
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        order.setOrderStatus(OrderStatus.INITIATED.toString());
        order = orderRepository.saveAndFlush(order);
        log.info("Order saved successfully with id:{}", order.getOrderId());

        OrderResponseDTO orderResponseDTO = new OrderResponseDTO(order.getOrderId(), order.getProductId(), order.getQuantity());
        return new ResponseEntity<>(orderResponseDTO, HttpStatus.CREATED);
    }

    public ResponseEntity<?> updateOrderStatus(OrderUpdateRequestDTO orderUpdateRequestDTO) {
        if (orderUpdateRequestDTO.getOrderId() == null || orderUpdateRequestDTO.getOrderId() == 0) {
            log.error("Order update failed, order id is null or 0");
            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderStatus.FAILED.toString(), "Order update failed, order id cannot be null or 0", LocalDateTime.now().toString());
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        }

        if (orderUpdateRequestDTO.getOrderStatus() == null) {
            log.error("Order update failed, orderStatus is null");
            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderStatus.FAILED.toString(), "Order update failed, order status cannot be null", LocalDateTime.now().toString());
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        }

        int numberOfUpdates = orderRepository.updateOrderStatusAndModificationReasonByOrderId(orderUpdateRequestDTO.getOrderStatus().toString(), orderUpdateRequestDTO.getModificationReason().toString() ,orderUpdateRequestDTO.getOrderId());
        log.info("Updated order status to:{} of {} records", numberOfUpdates, orderUpdateRequestDTO.getOrderStatus());

        return new ResponseEntity<>(OrderStatus.UPDATED, HttpStatus.OK);
    }
}
