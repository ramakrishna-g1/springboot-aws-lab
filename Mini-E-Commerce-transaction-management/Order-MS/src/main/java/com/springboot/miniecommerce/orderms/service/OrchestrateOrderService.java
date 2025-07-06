package com.springboot.miniecommerce.orderms.service;

import com.springboot.miniecommerce.commonutils.constant.InventoryEnum;
import com.springboot.miniecommerce.commonutils.constant.ModificationReason;
import com.springboot.miniecommerce.commonutils.constant.OrderEnum;
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

        if (order.getProductId() == null || order.getProductId() == 0) {
            log.error("Order received does not specify productId in it");
            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.PRODUCT_ID_NULL.toString(), "Product ID supplied for order creation is invalid", LocalDateTime.now().toString());
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        }

        if (order.getQuantity() == null || order.getQuantity() <= 0) {
            log.error("Order is for invalid quantity");
            ErrorDTO error = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), OrderEnum.INVALID_QUANTITY.toString(), "Quantity given for order is invalid", LocalDateTime.now().toString());
            return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
        }

        OrderRequestDTO orderRequestDTO = new OrderRequestDTO(order.getProductId(), order.getQuantity(), ModificationReason.NEW_ORDER_CREATION);
        ResponseEntity<?> orderResponse = orderService.createOrder(orderRequestDTO);

        log.info("Received status from Order transaction:{}", orderResponse.getStatusCode().value());
        if (orderResponse.getStatusCode() == HttpStatus.CREATED) {
            if (!orderResponse.hasBody()) {
                log.error("Order response body is null");
                ErrorDTO error = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), OrderEnum.FAILED.toString(), "Order response body is null", LocalDateTime.now().toString());
                return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
            }

            OrderResponseDTO orderResponseDTO = (OrderResponseDTO) orderResponse.getBody();
            log.info("Order with id:{} created", orderResponseDTO.getOrderId());

            ResponseEntity<?> inventoryResponse = updateInventory(orderResponseDTO.getOrderId(), orderResponseDTO.getProductId(), orderResponseDTO.getQuantity());
            log.info("Received status from Inventory transaction:{}", inventoryResponse.getStatusCode().value());

            if (!inventoryResponse.hasBody()) {
                //TODO To retry inventoryUpdate or not?
                log.warn("Inventory response body is null, triggering order creation compensation");
                compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_INVENTORY_FAILURE);

                ErrorDTO error = new ErrorDTO(inventoryResponse.getStatusCode().value(), InventoryEnum.INVENTORY_RESPONSE_NULL.toString(), "Inventory response body is null", LocalDateTime.now().toString());
                return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
            }

            if (inventoryResponse.getStatusCode() == HttpStatus.OK) {
                InventoryResponseDTO inventoryResponseDTO = (InventoryResponseDTO) inventoryResponse.getBody();
                log.info("total amount for orderId:{} is:{}", orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                ResponseEntity<?> paymentResponse = makePayment(orderResponseDTO.getOrderId(), inventoryResponseDTO.getAmount());

                if (!paymentResponse.hasBody()) {
                    log.warn("payment response body is null, compensating order creation");
                    compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_PAYMENT_FAILURE);

                    log.warn("payment response body is null, compensating inventory update");
                    compensateInventoryUpdate(orderResponseDTO.getOrderId(), orderResponseDTO.getProductId(), orderResponseDTO.getQuantity(), ModificationReason.COMPENSATE_PAYMENT_FAILURE);

                    ErrorDTO error = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), PaymentEnum.PAYMENT_RESPONSE_NULL.toString(), "Payment response was invalid", LocalDateTime.now().toString());
                    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
                }

                log.info("Received status from payment transaction:{}", paymentResponse.getStatusCode().value());

                if (paymentResponse.getStatusCode() == HttpStatus.OK) {
                    OrderUpdateRequestDTO orderUpdateRequest = new OrderUpdateRequestDTO(orderResponseDTO.getOrderId(), OrderEnum.CONFIRMED, ModificationReason.PAYMENT_SUCCESSFUL);
                    ResponseEntity<?> orderUpdateResponse = orderService.updateOrderStatus(orderUpdateRequest);

                    if (orderUpdateResponse.getStatusCode() == HttpStatus.OK) {
                        log.info("Order status for orderId:{} updated to:{}", orderResponseDTO.getOrderId(), OrderEnum.CONFIRMED);
                        return new ResponseEntity<>(orderResponseDTO, HttpStatus.OK);
                    } else {
                        //TODO Retry order update
                        log.error("status code:{} received while updating order status to CONFIRMED, error info:{}", orderUpdateResponse.getStatusCode(), orderUpdateResponse.getBody());
                    }
                } else {
                    //TODO handle exception from payments service
                    log.error("status code:{} received while making payment, error info:{}", paymentResponse.getStatusCode(), paymentResponse.getBody());

                    compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_PAYMENT_FAILURE);
                    compensateInventoryUpdate(orderResponseDTO.getOrderId(), orderResponseDTO.getProductId(), orderResponseDTO.getQuantity(), ModificationReason.COMPENSATE_PAYMENT_FAILURE);
                    return paymentResponse;
                }
            } else {
                //TODO handle exception from inventory service
                log.error("Received status while updating inventory:{}, error info:{}", inventoryResponse.getStatusCode(), inventoryResponse.getBody());
                log.warn("Inventory update failed, triggering order creation compensation");

                compensateOrderCreation(orderResponseDTO.getOrderId(), ModificationReason.COMPENSATE_INVENTORY_FAILURE);
                return inventoryResponse;
            }
        } else {
            //TODO handle exception from order service
            log.error("status code:{} received while creating order, error info:{}", orderResponse.getStatusCode(), orderResponse.getBody());
            return orderResponse;
        }
        return null;
    }

    /**
     * Updates inventory by sending a PATCH request to Inventory service
     *
     * @param orderId   Inventory update for which order
     * @param productId Product ID to update
     * @param quantity  Quantity to update
     * @return Response entity containing {@link InventoryResponseDTO} (if success) or {@link ErrorDTO} (if failed)
     */
    private ResponseEntity<?> updateInventory(long orderId, long productId, int quantity) {
        try {
            InventoryRequestDTO inventoryRequestDTO = new InventoryRequestDTO(orderId, productId, quantity, ModificationReason.NEW_ORDER_CREATION);

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
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), InventoryEnum.INVENTORY_SERVICE_UNREACHABLE.toString(), "Exception while calling inventory service", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Makes payment by sending POST request to Payment microservice
     *
     * @param orderId     ID of order for which payment is to be made
     * @param orderAmount total order amount
     * @return Response entity containing {@link PaymentResponseDTO} (if success) or {@link ErrorDTO} (if failed)
     */
    private ResponseEntity<?> makePayment(long orderId, double orderAmount) {
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
            log.error("Payments service for orderId:{} responded with: {}", orderId, restException.getStatusCode(), restException);
            ErrorDTO errorDTO = restException.getResponseBodyAs(ErrorDTO.class);
            return new ResponseEntity<>(errorDTO, restException.getStatusCode());
        } catch (Exception e) {
            //TODO Retry and then send response back
            log.error("Exception while calling payment service", e);
            ErrorDTO errorDTO = new ErrorDTO(HttpStatus.INTERNAL_SERVER_ERROR.value(), PaymentEnum.PAYMENT_SERVICE_UNREACHABLE.toString(), "Exception while calling payment service", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorDTO, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    /**
     * Compensates Order creation by updating order status to "FAILED"
     *
     * @param orderId            ID of order to update status
     * @param modificationReason the reason of compensation
     */
    private void compensateOrderCreation(long orderId, ModificationReason modificationReason) {
        OrderUpdateRequestDTO orderUpdateRequest = new OrderUpdateRequestDTO(orderId, OrderEnum.FAILED, modificationReason);
        ResponseEntity<?> orderUpdateResponse = orderService.updateOrderStatus(orderUpdateRequest);

        if (orderUpdateResponse.getStatusCode().value() == HttpStatus.OK.value()) {
            log.info("Order compensation done for orderId:{} updated order status to:{}", orderId, OrderEnum.FAILED);
        } else {
            log.warn("Order compensation for orderId:{} failed with status:{}", orderId, orderUpdateResponse.getStatusCode().value());
            //TODO retry and throw exception if req
        }
    }

    /**
     * Send compensation request to Inventory service.
     *
     * @param orderId ID of order for which compensation is requested
     * @param productId ID of the product whose stock has to be updated
     * @param quantity Quantity of product that was made
     * @param modificationReason Reason of making compensation
     */
    private void compensateInventoryUpdate(long orderId, long productId, int quantity, ModificationReason modificationReason) {
        try {
            InventoryRequestDTO inventoryRevertDTO = new InventoryRequestDTO(orderId, productId, quantity, modificationReason);

            log.info("Calling inventory to compensate for orderId:{}", orderId);
            ResponseEntity<String> response=this.inventoryRestClient
                    .patch()
                    .uri("/{id}/revert", productId)
                    .body(inventoryRevertDTO)
                    .retrieve()
                    .toEntity(String.class);

            log.info("Inventory compensation returned status:{} and response:{}", response.getStatusCode(), response.getBody());
        } catch (RestClientResponseException restException) {
            log.error("Inventory service responded with: {}", restException.getStatusCode(), restException);
            ErrorDTO errorDTO = restException.getResponseBodyAs(ErrorDTO.class);
            //TODO throw exception or retry
            log.error("Error while compensating inventory update for orderId:{}, orderIdError info:{}", orderId, errorDTO);
        } catch (Exception exception) {
            log.error("Exception while compensating inventory update for orderId:{}", orderId, exception);
        }
    }

}
