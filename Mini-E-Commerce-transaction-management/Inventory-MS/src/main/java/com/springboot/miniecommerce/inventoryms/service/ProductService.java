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

    public ResponseEntity<?> updateProductInventory(Long productId, InventoryRequestDTO inventoryRequestDTO) {

        if (productId == null || productId == 0) {
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_ID_NULL.toString(), "ProductId is required for updating product inventory", LocalDateTime.now().toString());
            return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
        }

        if (inventoryRequestDTO.getRequestedQuantity() == 0) {
            ErrorDTO errorResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.PRODUCT_ID_NULL.toString(), "Quantity of product is required for updating product inventory", LocalDateTime.now().toString());
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

        if (product.getAvailableQuantity() < inventoryRequestDTO.getRequestedQuantity()) {
            log.error("Order placed for more quantity than available");
            ErrorDTO apiResponse = new ErrorDTO(HttpStatus.BAD_REQUEST.value(), InventoryEnum.INSUFFICIENT_INVENTORY.toString(), "Order placed for more quantity than available", LocalDateTime.now().toString());
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }
        //Update availableQuantity of product after order Completion
        product.setAvailableQuantity(product.getAvailableQuantity() - inventoryRequestDTO.getRequestedQuantity());
        double orderPrice = product.getPrice() * inventoryRequestDTO.getRequestedQuantity();

        //TODO Use orderId came with request so that you can track the inventory updates
        productRepository.saveAndFlush(product);

        InventoryResponseDTO inventoryResponseDTO = new InventoryResponseDTO(product.getProductId(), orderPrice);
        return new ResponseEntity<>(inventoryResponseDTO, HttpStatus.OK);
    }

}
