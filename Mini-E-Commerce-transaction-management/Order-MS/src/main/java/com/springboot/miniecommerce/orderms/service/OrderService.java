package com.springboot.miniecommerce.orderms.service;

import com.springboot.miniecommerce.commonutils.dto.ApiResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.OrderUpdateRequestDTO;
import com.springboot.miniecommerce.commonutils.constant.OrderStatus;
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

    public ResponseEntity<ApiResponseDTO<?>> createOrder(Order order) {
        if (order.getProductId() == null || order.getProductId() == 0) {
            ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(OrderStatus.FAILED.toString(), "Order creation failed, product Id is null");
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        order.setOrderStatus(OrderStatus.INITIATED.toString());
        order = orderRepository.saveAndFlush(order);
        log.info("Order saved successfully with id:{}", order.getOrderId());

        OrderResponseDTO orderResponseDTO = new OrderResponseDTO(order.getOrderId(), order.getProductId(), order.getQuantity());
        ApiResponseDTO<OrderResponseDTO> apiResponse = new ApiResponseDTO<>(OrderStatus.INITIATED.toString(), "Order initiated successfully", orderResponseDTO);

        return new ResponseEntity<>(apiResponse, HttpStatus.CREATED);
    }

    public ResponseEntity<ApiResponseDTO<?>> updateOrderStatus(OrderUpdateRequestDTO orderUpdateRequestDTO) {
        if (orderUpdateRequestDTO.getOrderId() == null || orderUpdateRequestDTO.getOrderId() == 0) {
            ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(OrderStatus.FAILED.toString(), "Order update failed, order id is null");
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        int numberOfUpdates = orderRepository.updateOrderStatusByOrderId(orderUpdateRequestDTO.getOrderStatus().toString(), orderUpdateRequestDTO.getOrderId());
        log.info("Updated order status to:{} of {} records", numberOfUpdates, orderUpdateRequestDTO.getOrderStatus());

        ApiResponseDTO<OrderUpdateRequestDTO> apiResponse = new ApiResponseDTO<>(OrderStatus.UPDATED.toString(), "Order updated successfully", orderUpdateRequestDTO);
        return new ResponseEntity<>(apiResponse, HttpStatus.OK);
    }
}
