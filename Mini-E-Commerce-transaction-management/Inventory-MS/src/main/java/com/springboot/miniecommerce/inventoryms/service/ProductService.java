package com.springboot.miniecommerce.inventoryms.service;

import com.springboot.miniecommerce.commonutils.constant.InventoryUpdateStatus;
import com.springboot.miniecommerce.commonutils.dto.ApiResponseDTO;
import com.springboot.miniecommerce.commonutils.dto.InventoryRequestDTO;
import com.springboot.miniecommerce.commonutils.dto.InventoryResponseDTO;
import com.springboot.miniecommerce.inventoryms.model.Product;
import com.springboot.miniecommerce.inventoryms.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class ProductService {

    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private final ProductRepository productRepository;

    public ProductService(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    public ResponseEntity<ApiResponseDTO<?>> updateProductInventory(Long productId, InventoryRequestDTO inventoryRequestDTO) {

        if (productId == null || productId == 0) {
            ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(InventoryUpdateStatus.FAILED.toString(), "Product inventory update failed");
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }

        log.info("Get product with productId {}", productId);
        Optional<Product> productOptional = productRepository.getProductsByProductId(productId);

        if (productOptional.isEmpty()) {
            log.error("No product found with id: {}", productId);

            ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(InventoryUpdateStatus.FAILED.toString(), "No such Product found");
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }
        Product product = productOptional.get();
        log.info("Current available quantity for productId:{} is:{}", product.getProductId(), product.getAvailableQuantity());

        if (product.getAvailableQuantity() < inventoryRequestDTO.getQuantity()) {
            log.error("Order placed for more quantity than available");
            ApiResponseDTO<Void> apiResponse = new ApiResponseDTO<>(InventoryUpdateStatus.FAILED.toString(), "Order placed for more quantity than available");
            return new ResponseEntity<>(apiResponse, HttpStatus.BAD_REQUEST);
        }
        product.setAvailableQuantity(product.getAvailableQuantity() - inventoryRequestDTO.getQuantity());
        double orderPrice = product.getPrice() * inventoryRequestDTO.getQuantity();

        productRepository.saveAndFlush(product);

        InventoryResponseDTO inventoryResponseDTO = new InventoryResponseDTO(product.getProductId(), orderPrice);
        ApiResponseDTO<InventoryResponseDTO> apiResponse =
                new ApiResponseDTO<>(InventoryUpdateStatus.SUCCESS.toString(), "Product updated successfully", inventoryResponseDTO);

        return new ResponseEntity<>(apiResponse, HttpStatus.OK);
    }

    public ResponseEntity<ApiResponseDTO<?>> addProducts(List<Product> products) {

        if (products.isEmpty()) {
            ApiResponseDTO<Void> apiresponse = new ApiResponseDTO<>("ERROR", "No products given to add");
            return new ResponseEntity<>(apiresponse, HttpStatus.BAD_REQUEST);
        }

        productRepository.saveAll(products);

        ApiResponseDTO<Void> apiresponse = new ApiResponseDTO<>("ADDED", "Products added successfully");
        return new ResponseEntity<>(apiresponse, HttpStatus.OK);
    }
}
