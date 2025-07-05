package com.springboot.miniecommerce.inventoryms.repository;

import com.springboot.miniecommerce.inventoryms.model.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Integer> {

    @Query(nativeQuery = true, value = "select available_quantity from product where product_id= :productId")
    Optional<Integer> getAvailableQuantityByProductId(long productId);

    Optional<Product> getProductsByProductId(long productId);

    @Transactional
    @Modifying
    @Query(nativeQuery = true, value = "update product set available_quantity = :availableQuantity where product_id = :productId")
    int updateAvailableQuantityByProductId(long productId, int availableQuantity);
}
