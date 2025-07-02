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
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

@Service
public class OrchestrateOrderService {

    private static final Logger log = LoggerFactory.getLogger(OrchestrateOrderService.class);

    @Value("${service.inventory.base-url}")
    private String inventoryServiceBaseUrl;

    @Value("${service.payment.base-url}")
    private String paymentServiceBaseUrl;

    private final OrderService orderService;

    private final RestClient inventoryRestClient;
    private final RestClient paymentRestClient;

    public OrchestrateOrderService(OrderService orderService, RestClient.Builder restClientBuilder) {
        this.inventoryRestClient = restClientBuilder.baseUrl(inventoryServiceBaseUrl).build();
        this.paymentRestClient = restClientBuilder.baseUrl(paymentServiceBaseUrl).build();
        this.orderService = orderService;
    }

    public ResponseEntity<String> orchestrateOrderCreation(Order order) {
        ResponseEntity<ApiResponseDTO<?>> orderResponse = orderService.createOrder(order);

        log.info("Received status from Order transaction:{}, status:{}, message:{}", orderResponse.getStatusCode().value(), orderResponse.getBody().getStatus(), orderResponse.getBody().getMessage());
        if (orderResponse.getStatusCode() == HttpStatus.CREATED) {
            if (orderResponse.getBody() == null) {
                return new ResponseEntity<>("Order response body is null", HttpStatus.INTERNAL_SERVER_ERROR); //TODO update body and code
            }

            OrderResponseDTO orderResponseDTO = (OrderResponseDTO) orderResponse.getBody().getData();

            InventoryRequestDTO inventoryRequestDTO = new InventoryRequestDTO(orderResponseDTO.getProductId(), orderResponseDTO.getQuantity());

            log.info("Calling Inventory service");
            ResponseEntity<ApiResponseDTO<InventoryResponseDTO>> inventoryResponse = this.inventoryRestClient
                    .patch()
                    .uri("/{id}", orderResponseDTO.getProductId())
                    .body(inventoryRequestDTO)
                    .exchange((request, response) -> {
                        ObjectMapper objectMapper = new ObjectMapper();
                        try (InputStream bodyStream = response.getBody()) {
                            ApiResponseDTO<InventoryResponseDTO> apiResponse =
                                    objectMapper.readValue(bodyStream, new TypeReference<ApiResponseDTO<InventoryResponseDTO>>() {
                                    });
                            return new ResponseEntity<>(apiResponse, response.getStatusCode());
                        } catch (IOException exception) {
                            throw new InventoryUpdateFailedException(response.getStatusCode().value(), InventoryUpdateStatus.FAILED.toString(), "Exception while reading response from Inventory API");
                        }
                    });

            if (Objects.isNull(inventoryResponse) || !inventoryResponse.hasBody()) {
                return new ResponseEntity<>("Inventory response body is null", HttpStatus.INTERNAL_SERVER_ERROR); //TODO revert order creation
            }
            log.info("Received status from inventory transaction:{}, status:{}, message:{}", inventoryResponse.getStatusCode().value(), inventoryResponse.getBody().getStatus(), inventoryResponse.getBody().getMessage());

            if (inventoryResponse.getStatusCode().value() == HttpStatus.OK.value()) {
                InventoryResponseDTO inventoryResponseDTO = inventoryResponse.getBody().getData();
                PaymentRequestDTO paymentRequestDTO = new PaymentRequestDTO(orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                log.info("Calling Payments service");
                ResponseEntity<ApiResponseDTO<PaymentResponseDTO>> paymentResponse = this.paymentRestClient
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

                if (Objects.isNull(paymentResponse) || !paymentResponse.hasBody()) {
                    return new ResponseEntity<>("Payment response body is null", HttpStatus.INTERNAL_SERVER_ERROR); //TODO revert order creation
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
                log.error("Exception while updating inventory");
            }
        } else {
            //TODO handle exception from order service
            log.error("Exception while creating order");
        }
        return null;
    }
}
