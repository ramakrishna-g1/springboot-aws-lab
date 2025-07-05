package com.springboot.miniecommerce.orderms.service;

import com.springboot.miniecommerce.commonutils.constant.OrderEnum;
import com.springboot.miniecommerce.commonutils.dto.OrderRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderUpdateRequestDTO;
import com.springboot.miniecommerce.orderms.model.Order;
import com.springboot.miniecommerce.orderms.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class OrderService {


    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public ResponseEntity<?> createOrder(OrderRequestDTO orderRequest) {
//        Validation not required since validations are done by orchestrator service in same MS
//        if (orderRequest.getProductId() == 0) {
//            log.info("Order creation failed, product Id is invalid");
//            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.FAILED.toString(), "Order creation failed, product Id is invalid", LocalDateTime.now().toString());
//            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
//        }
//        if (orderRequest.getQuantity() <= 0) {
//            log.info("Order creation failed, quantity is invalid");
//            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.FAILED.toString(), "Order creation failed, quantity is invalid", LocalDateTime.now().toString());
//            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
//        }

        Order order = new Order();
        order.setProductId(orderRequest.getProductId());
        order.setQuantity(orderRequest.getQuantity());
        order.setModificationReason(orderRequest.getModificationReason().toString());
        order.setOrderStatus(OrderEnum.INITIATED.toString());

        order = orderRepository.saveAndFlush(order);
        log.info("Order saved successfully with id:{}", order.getOrderId());

        OrderResponseDTO orderResponseDTO = new OrderResponseDTO(order.getOrderId(), order.getProductId(), order.getQuantity());
        return new ResponseEntity<>(orderResponseDTO, HttpStatus.CREATED);
    }

    public ResponseEntity<?> updateOrderStatus(OrderUpdateRequestDTO orderUpdateRequestDTO) {
//        Validation not required since validations are done by orchestrator service in same MS
//        if (orderUpdateRequestDTO.getOrderId() == 0) {
//            log.error("Order update failed, order id is null or 0");
//            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.FAILED.toString(), "Order update failed, order id cannot be null or 0", LocalDateTime.now().toString());
//            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
//        }
//
//        if (orderUpdateRequestDTO.getOrderStatus() == null) {
//            log.error("Order update failed, orderStatus is null");
//            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.FAILED.toString(), "Order update failed, order status cannot be null", LocalDateTime.now().toString());
//            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
//        }

        log.info("Updating order status for orderId:{} to:{}", orderUpdateRequestDTO.getOrderId(), orderUpdateRequestDTO.getOrderStatus());
        int numberOfUpdates = orderRepository.updateOrderStatusAndModificationReasonByOrderId(orderUpdateRequestDTO.getOrderStatus().toString(), orderUpdateRequestDTO.getModificationReason().toString(), orderUpdateRequestDTO.getOrderId());
        log.info("Updated order status to:{} of {} records", orderUpdateRequestDTO.getOrderStatus(), numberOfUpdates);

        return new ResponseEntity<>(OrderEnum.UPDATED, HttpStatus.OK);
    }
}
