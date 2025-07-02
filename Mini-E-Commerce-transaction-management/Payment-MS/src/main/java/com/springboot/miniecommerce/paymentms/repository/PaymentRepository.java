package com.springboot.miniecommerce.paymentms.repository;

import com.springboot.miniecommerce.paymentms.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, Long> {

}
