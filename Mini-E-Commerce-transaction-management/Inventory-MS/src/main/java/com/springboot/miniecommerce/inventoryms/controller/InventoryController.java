package com.springboot.miniecommerce.inventoryms.controller;

import com.springboot.miniecommerce.commonutils.dto.ErrorDTO;
import com.springboot.miniecommerce.commonutils.dto.InventoryRequestDTO;
import com.springboot.miniecommerce.inventoryms.model.Product;
import com.springboot.miniecommerce.inventoryms.service.ProductService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final ProductService productService;

    public InventoryController(ProductService productService) {
        this.productService = productService;
    }

    @PostMapping("/product")
    public ResponseEntity<?> addProducts(@RequestBody List<Product> products) {
        return productService.addProducts(products);
    }

    @PatchMapping("/{id}")
    public ResponseEntity<?> updateInventory(@PathVariable("id") Long productId, @RequestBody InventoryRequestDTO inventoryRequestDTO){
        return productService.updateProductInventory(productId, inventoryRequestDTO);
    }
}
