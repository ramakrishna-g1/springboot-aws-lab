package com.springboot.miniecommerce.inventoryms.service;

import com.springboot.miniecommerce.commonutils.constant.InventoryEnum;
import com.springboot.miniecommerce.commonutils.dto.ErrorDTO;
import com.springboot.miniecommerce.commonutils.dto.InventoryRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.InventoryResponseDTO;
import com.springboot.miniecommerce.inventoryms.model.Product;
import com.springboot.miniecommerce.inventoryms.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Service
public class ProductService {

    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private final ProductRepository productRepository;

    public ProductService(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }


    public ResponseEntity<?> addProducts(List<Product> products) {
        if (products.isEmpty()) {
            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.NO_PRODUCTS_SUPPLIED.toString(), "No products given to add", LocalDateTime.now().toString());
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        productRepository.saveAll(products);

        return new ResponseEntity<>(InventoryEnum.PRODUCT_ADDED, HttpStatus.OK);
    }

    public ResponseEntity<?> updateProductInventory(long productId, InventoryRequestDTO inventoryRequestDTO) {
        if (productId == 0) {
            log.error("Product Id given is invalid:{}", productId);
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_ID_NULL.toString(), "ProductId is required for updating product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        if (inventoryRequestDTO.getProductQuantity() == 0) {
            log.error("product quantity Id given is invalid:{}", inventoryRequestDTO.getProductQuantity());
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_QUANTITY_NULL.toString(), "Quantity of product is required for updating product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        log.info("Get product with productId {}", productId);
        Optional<Product> productOptional = productRepository.getProductsByProductId(productId);

        if (productOptional.isEmpty()) {
            log.error("No product found with id: {}", productId);
            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.NOT_FOUND.value(), InventoryEnum.PRODUCT_NOT_FOUND.toString(), ("Product with productId:" + productId + " not found"), LocalDateTime.now().toString());
            return new ResponseEntity<>(apiResponse, HttpStatus.NOT_FOUND);
        }

        Product product = productOptional.get();
        log.info("Current available quantity for productId:{} is:{}", product.getProductId(), product.getAvailableQuantity());

        if (product.getAvailableQuantity() < inventoryRequestDTO.getProductQuantity()) {
            log.error("Order placed for more quantity than available");
            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.INSUFFICIENT_INVENTORY.toString(), "Order placed for more quantity than available", LocalDateTime.now().toString());
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        //Update availableQuantity of product
        product.setAvailableQuantity(product.getAvailableQuantity() - inventoryRequestDTO.getProductQuantity());
        double orderPrice = product.getPrice() * inventoryRequestDTO.getProductQuantity();

        productRepository.saveAndFlush(product);
        log.info("Inventory updated for productId:{} orderId:{}", inventoryRequestDTO.getProductId(), inventoryRequestDTO.getOrderId());
        //TODO Save transaction detail into InventoryUpdate_AuditTrail table, with Order ID and Modification Reason.

        InventoryResponseDTO inventoryResponseDTO = new InventoryResponseDTO(product.getProductId(), orderPrice);
        return new ResponseEntity<>(inventoryResponseDTO, HttpStatus.OK);
    }

    public ResponseEntity<?> revertInventoryUpdate(long productId, InventoryRequestDTO inventoryRequestDTO) {
        if (productId == 0) {
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_ID_NULL.toString(), "ProductId is required for updating product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        if (inventoryRequestDTO.getProductQuantity() == 0) {
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_QUANTITY_NULL.toString(), "Quantity of product is required for updating product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        Optional<Integer> currentQuantityOptional = productRepository.getAvailableQuantityByProductId(productId);

        if(currentQuantityOptional.isEmpty()) {
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_NOT_FOUND.toString(), "ProductId not found in product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        log.info("Current quantity of ProductId:{} is:{}", productId, currentQuantityOptional.get());

        int revertToQuantity = currentQuantityOptional.get() + inventoryRequestDTO.getProductQuantity();

        int recordsUpdated = productRepository.updateAvailableQuantityByProductId(productId, revertToQuantity);
        log.info("Quantity reverted to:{}", revertToQuantity);

        log.info("Reverted inventory update for productId:{} orderId:{} recordsUpdated:{}", inventoryRequestDTO.getProductId(), inventoryRequestDTO.getOrderId(), recordsUpdated);
        //TODO Save transaction detail into InventoryUpdate_AuditTrail table, with Order ID and Modification Reason.

        return new ResponseEntity<>(InventoryEnum.INVENTORY_COMPENSATION_SUCCESSFUL.toString(), HttpStatus.OK);
    }

}
