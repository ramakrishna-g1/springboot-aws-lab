package com.springboot.miniecommerce.inventoryms.repository;

import com.springboot.miniecommerce.inventoryms.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {

    Optional<Long> getQuantityByProductId(Long productId);

    Optional<Product> getProductsByProductId(Long productId);
}
