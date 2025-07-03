package com.springboot.miniecommerce.orderms.service;

import com.springboot.miniecommerce.commonutils.constant.InventoryEnum;
import com.springboot.miniecommerce.commonutils.constant.ModificationReason;
import com.springboot.miniecommerce.commonutils.constant.OrderStatus;
import com.springboot.miniecommerce.commonutils.constant.PaymentEnum;
import com.springboot.miniecommerce.commonutils.dto.*;
import com.springboot.miniecommerce.orderms.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import java.time.LocalDateTime;

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

    public ResponseEntity<?> orchestrateOrderCreation(Order order) {
        ResponseEntity<?> orderResponse = orderService.createOrder(order);

        log.info("Received status from Order transaction:{}", orderResponse.getStatusCode().value());
        if (orderResponse.getStatusCode() == HttpStatus.CREATED) {
            if (!orderResponse.hasBody()) {
                log.error("Order response body is null");
                ErrorDTO error = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), OrderStatus.FAILED.toString(), "Order response body is null", LocalDateTime.now().toString());
                return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR); //TODO update body and code
            }

            OrderResponseDTO orderResponseDTO = (OrderResponseDTO) orderResponse.getBody();
            log.info("Order with id:{} created", orderResponseDTO.getOrderId());

            ResponseEntity<?> inventoryResponse = updateInventory(orderResponseDTO.getOrderId(), orderResponseDTO.getProductId(), orderResponseDTO.getQuantity());

            if (!inventoryResponse.hasBody()) {
                //TODO To retry inventoryUpdate or not
                log.warn("Inventory response body is null, triggering order creation compensation");
                compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_INVENTORY_FAILURE);

                ErrorDTO error = new ErrorDTO(inventoryResponse.getStatusCode().value(), InventoryEnum.INVENTORY_RESPONSE_NULL.toString(), "Inventory response body is null", LocalDateTime.now().toString());
                return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
            }
            log.info("Received status from Inventory transaction:{}", inventoryResponse.getStatusCode().value());

            if (inventoryResponse.getStatusCode() == HttpStatus.OK) {
                InventoryResponseDTO inventoryResponseDTO = (InventoryResponseDTO) inventoryResponse.getBody();
                log.info("Total amount for orderId:{} is:{}", orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                ResponseEntity<?> paymentResponse = makePayment(orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                if (!paymentResponse.hasBody()) {
                    //TODO revert order and inventory creation
                    log.warn("Payment response body is null, triggering inventory update compensation");
//                   TODO compensateInventoryUpdate(orderResponseDTO.getProductId(), orderResponseDTO.getQuantity());
                    compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_PAYMENT_FAILURE);

                    ErrorDTO error = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), PaymentEnum.PAYMENT_RESPONSE_NULL.toString(), "Payment response body is null", LocalDateTime.now().toString());
                    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
                }

                log.info("Received status from payment transaction:{}", paymentResponse.getStatusCode().value());

                if (paymentResponse.getStatusCode().value() == HttpStatus.OK.value()) {
                    OrderUpdateRequestDTO orderUpdateRequest = new OrderUpdateRequestDTO(orderResponseDTO.getOrderId(), OrderStatus.CONFIRMED, ModificationReason.PAYMENT_SUCCESSFUL);
                    ResponseEntity<?> response = orderService.updateOrderStatus(orderUpdateRequest);

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
                log.error("Received status while updating inventory:{}", inventoryResponse.getStatusCode());
                log.warn("Inventory update failed, triggering order creation compensation");

                compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_INVENTORY_FAILURE);
                return inventoryResponse;
            }
        } else {
            //TODO handle exception from order service
            log.error("Exception while creating order");
        }
        return null;
    }

    private void compensateOrderCreation(Long orderId, ModificationReason modificationReason) {
        OrderUpdateRequestDTO orderUpdateRequest = new OrderUpdateRequestDTO(orderId, OrderStatus.FAILED, modificationReason);
        ResponseEntity<?> orderUpdateResponse = orderService.updateOrderStatus(orderUpdateRequest);

        if (orderUpdateResponse.getStatusCode().value() == HttpStatus.OK.value()) {
            log.info("Order status for orderId:{} updated to:{}", orderId, OrderStatus.FAILED);
        } else {
            log.warn("Order status for orderId:{} update failed with status:{}", orderId, orderUpdateResponse.getStatusCode().value());
        }
    }


    private void compensateInventoryUpdate(Long productId, Integer quantity) {

    }

    private ResponseEntity<?> makePayment(Long orderId, double orderAmount) {
        try {
            PaymentRequestDTO paymentRequestDTO = new PaymentRequestDTO(orderId, orderAmount);

            log.info("Calling Payments service");
            return this.paymentRestClient
                    .post()
                    .uri("")
                    .body(paymentRequestDTO)
                    .retrieve()
                    .toEntity(PaymentResponseDTO.class);

        } catch (RestClientResponseException restException) {
            log.error("Payments service responded with: {}", restException.getStatusCode(), restException);
            ErrorDTO errorDTO = restException.getResponseBodyAs(ErrorDTO.class);
            return new ResponseEntity<>(errorDTO, restException.getStatusCode());
        } catch (Exception e) {
            //TODO Retry and then send response back
            log.error("Exception while calling payment service", e);
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), PaymentEnum.FAILED.toString(), "Exception while calling payment service", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    /**
     * Updates inventory by sending a PATCH request to Inventory service
     *
     * @param orderId   Inventory update for which order
     * @param productId Product ID to update
     * @param quantity  Quantity to update
     * @return Response entity containing inventory update status
     */
    private ResponseEntity<?> updateInventory(Long orderId, Long productId, Integer quantity) {
        try {
            InventoryRequestDTO inventoryRequestDTO = new InventoryRequestDTO(orderId, productId, quantity);

            log.info("Calling Inventory service");
            return this.inventoryRestClient
                    .patch()
                    .uri("/{id}", productId)
                    .body(inventoryRequestDTO)
                    .retrieve()
                    .toEntity(InventoryResponseDTO.class);

        } catch (RestClientResponseException restException) {
            log.error("Inventory service responded with: {}", restException.getStatusCode(), restException);
            ErrorDTO errorDTO = restException.getResponseBodyAs(ErrorDTO.class);
            return new ResponseEntity<>(errorDTO, restException.getStatusCode());
        } catch (Exception e) {
            //TODO Retry and then send response back
            log.error("Exception while calling inventory service", e);
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), InventoryEnum.FAILED.toString(), "Exception while calling inventory service", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
