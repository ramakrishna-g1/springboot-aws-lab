package com.springboot.miniecommerce.orderms.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springboot.miniecommerce.commonutils.constant.InventoryUpdateStatus;
import com.springboot.miniecommerce.commonutils.constant.OrderStatus;
import com.springboot.miniecommerce.commonutils.constant.PaymentStatus;
import com.springboot.miniecommerce.commonutils.dto.*;
import com.springboot.miniecommerce.commonutils.exception.InventoryUpdateFailedException;
import com.springboot.miniecommerce.commonutils.exception.PaymentFailedException;
import com.springboot.miniecommerce.orderms.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Service
public class OrchestrateOrderService {

    private static final Logger log = LoggerFactory.getLogger(OrchestrateOrderService.class);


    private final OrderService orderService;

    private final RestClient inventoryRestClient;
    private final RestClient paymentRestClient;

    public OrchestrateOrderService(OrderService orderService, RestClient.Builder restClientBuilder,
                                   @Value("${service.inventory.base-url}") String inventoryServiceBaseUrl,
                                   @Value("${service.payment.base-url}") String paymentServiceBaseUrl) {

        log.info("inventoryServiceBaseUrl:{} paymentServiceBaseUrl:{}", inventoryServiceBaseUrl, paymentServiceBaseUrl);
        this.inventoryRestClient = restClientBuilder.baseUrl(inventoryServiceBaseUrl).build();
        this.paymentRestClient = restClientBuilder.baseUrl(paymentServiceBaseUrl).build();
        this.orderService = orderService;
    }

    public ResponseEntity<ApiResponseDTO<?>> orchestrateOrderCreation(Order order) {
        ResponseEntity<ApiResponseDTO<?>> orderResponse = orderService.createOrder(order);

        log.info("Received status from Order transaction:{}", orderResponse.getStatusCode().value());
        if (orderResponse.getStatusCode() == HttpStatus.CREATED) {
            if (!orderResponse.hasBody()) {
                ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(OrderStatus.FAILED.toString(), "Order response body is null");
                return new ResponseEntity<>(apiResponse, HttpStatus.INTERNAL_SERVER_ERROR); //TODO update body and code
            }
            log.info("Order status:{}, message:{}", orderResponse.getBody().getStatus(), orderResponse.getBody().getMessage());

            OrderResponseDTO orderResponseDTO = (OrderResponseDTO) orderResponse.getBody().getData();

            ResponseEntity<ApiResponseDTO<InventoryResponseDTO>> inventoryResponse = updateInventory(orderResponseDTO.getProductId(), orderResponseDTO.getQuantity());

            if (Objects.isNull(inventoryResponse) || !inventoryResponse.hasBody()) {
                ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(InventoryUpdateStatus.FAILED.toString(), "Inventory response body is null");
                return new ResponseEntity<>(apiResponse, HttpStatus.INTERNAL_SERVER_ERROR); //TODO revert order creation
            }
            log.info("Received status from inventory transaction:{}, status:{}, message:{}", inventoryResponse.getStatusCode().value(), inventoryResponse.getBody().getStatus(), inventoryResponse.getBody().getMessage());

            if (inventoryResponse.getStatusCode().value() == HttpStatus.OK.value()) {
                InventoryResponseDTO inventoryResponseDTO = inventoryResponse.getBody().getData();
                ResponseEntity<ApiResponseDTO<PaymentResponseDTO>> paymentResponse = makePayment(orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                if (Objects.isNull(paymentResponse) || !paymentResponse.hasBody()) {
                    ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(InventoryUpdateStatus.FAILED.toString(), "Payment response body is null");
                    return new ResponseEntity<>(apiResponse, HttpStatus.INTERNAL_SERVER_ERROR); //TODO revert order and inventory creation
                }

                log.info("Received status from payment transaction:{}, status:{}, message:{}", paymentResponse.getStatusCode().value(), paymentResponse.getBody().getStatus(), paymentResponse.getBody().getMessage());

                if (paymentResponse.getStatusCode().value() == HttpStatus.OK.value()) {
                    OrderUpdateRequestDTO orderUpdateRequest = new OrderUpdateRequestDTO(orderResponseDTO.getOrderId(), OrderStatus.CONFIRMED);
                    ResponseEntity<ApiResponseDTO<?>> response = orderService.updateOrderStatus(orderUpdateRequest);

                    if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                        log.info("Order status for orderId:{} update to:{}", orderResponseDTO.getOrderId(), OrderStatus.CONFIRMED);

                    } else {
                        //TODO handle exception from order service
                        log.error("Exception while updating order status to CONFIRMED");
                    }
                } else {
                    //TODO handle exception from payments service
                    log.error("Exception while doing payment");
                }
            } else {
                //TODO handle exception from inventory service
                log.error("Exception while updating inventory:{}", inventoryResponse.getBody());
            }
        } else {
            //TODO handle exception from order service
            log.error("Exception while creating order");
        }
        return null;
    }

    private ResponseEntity<ApiResponseDTO<PaymentResponseDTO>> makePayment(Long orderId, double orderAmount) {
        PaymentRequestDTO paymentRequestDTO = new PaymentRequestDTO(orderId, orderAmount);

        log.info("Calling Payments service");
        return this.paymentRestClient
                .post()
                .uri("")
                .body(paymentRequestDTO)
                .exchange((request, response) -> {
                    ObjectMapper objectMapper = new ObjectMapper();
                    try (InputStream bodyStream = response.getBody()) {
                        ApiResponseDTO<PaymentResponseDTO> apiResponse =
                                objectMapper.readValue(bodyStream, new TypeReference<ApiResponseDTO<PaymentResponseDTO>>() {
                                });
                        return new ResponseEntity<>(apiResponse, response.getStatusCode());
                    } catch (IOException exception) {
                        throw new PaymentFailedException(response.getStatusCode().value(), PaymentStatus.FAILED.toString(), "Exception while reading response from Payment API");
                    }
                });
    }

    /**
     * Updates inventory by sending a PATCH request to Inventory service
     *
     * @param productId Product ID to update
     * @param quantity  Quantity to update
     * @return Response entity containing inventory update status
     */
    private ResponseEntity<ApiResponseDTO<InventoryResponseDTO>> updateInventory(Long productId, Integer quantity) {
        try {
            InventoryRequestDTO inventoryRequestDTO = new InventoryRequestDTO(productId, quantity);

            log.info("Calling Inventory service");
            return this.inventoryRestClient
                    .patch()
                    .uri("/{id}", productId)
                    .body(inventoryRequestDTO)
                    .exchange((request, response) -> {
                        ObjectMapper objectMapper = new ObjectMapper();
                        InputStream bodyStream = response.getBody();
                        ApiResponseDTO<InventoryResponseDTO> apiResponse =
                                objectMapper.readValue(bodyStream, new TypeReference<ApiResponseDTO<InventoryResponseDTO>>() {
                                });
                        bodyStream.close();
                        return new ResponseEntity<>(apiResponse, response.getStatusCode());
                    });
        } catch (Exception e) {
            //TODO Retry and then send response back
            log.error("Exception in calling inventory service", e);

            return null;
        }
    }
}
