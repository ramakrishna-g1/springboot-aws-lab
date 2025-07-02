package com.springboot.miniecommerce.orderms.repository;

import com.springboot.miniecommerce.orderms.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    @Transactional
    @Modifying
    @Query(value = "update order_table set order_status = :orderStatus where order_id = :orderId", nativeQuery = true)
    int updateOrderStatusByOrderId(@Param("orderStatus") String orderStatus, @Param("orderId") Long orderId);
}
