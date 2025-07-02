# Mini E-Commerce application

## Goal
This project is a mini e-commerce application built using Spring Boot microservices. It demonstrates transaction management using the Saga pattern with orchestration. The application consists of three core services: Order Service, Inventory Service, and Payment Service.

## Design pattern used
We chose the **Saga pattern with orchestration** for managing distributed transactions across our microservices. In this pattern, a central orchestrator coordinates the transaction flow by invoking each service in sequence and handling success or failure scenarios.

### Why Saga Orchestration?
- **Centralized Control**: The orchestrator provides a clear view of the transaction flow, making it easier to manage and debug.
- **Robust Failure Handling**: Compensating actions (e.g., refunding payments, reverting inventory) are triggered by the orchestrator in case of failures.
- **Scalability**: The orchestrator can be scaled independently and optimized for performance.
- **Learning Purpose**: This pattern is ideal for understanding transaction management in distributed systems, especially in e-commerce scenarios where consistency and reliability are critical.
- Clear separation of concerns. 
- Easier to scale, monitor, and test independently.  
- Reusable for multiple workflows (e.g., order creation, returns, refunds).  

This design choice helped us explore real-world challenges in microservice communication and transaction consistency.

### Why not Saga Choreography?
- **Debugging**: It would be hard to track the requests, specially when there is no central manager/orchestrator.

1. [Order Microservice](Order-MS)- Order management microservice
   ## Order Service API Endpoints
    | Method | Endpoint                   | Description                                             |
    |:-------|:---------------------------|:--------------------------------------------------------|
    | GET    | `/api/order/{id}`          | Get details of an order                                 |
    | PATCH  | `/api/order/{id}/status`   | Update order status on successful completion            |
    | PATCH  | `/api/order/{id}/rollback` | Update order status in case of failures or cancellation |
    | PATCH  | `/api/order/{id}/cancel`   | For user to cancel the order                            |
    | POST   | `/api/orchestrator/order`  | Create an order. **Body:** `{}`                         |
NOTE: Below things are assumed while making DTOs. 1 order= 1 product  

2. [Inventory Microservice](Inventory-MS)-  Inventory management microservice
   ## Inventory Service API Endpoints
    | Method  | Endpoint                           | Description                               |
    |:--------|:-----------------------------------|:------------------------------------------|
    | GET     | `/api/inventory/{id}`              | Get inventory status of a product         |
    | PATCH   | `/api/inventory/{id}`              | Update inventory stock status             |
    | PATCH   | `/api/inventory/{id}/revert`       | Revert inventory stock info               |

3. [Payment Microservice](Payment-MS)-  Payment management microservice  
   ## Payment Service API Endpoints
    | Method  | Endpoint                           | Description                               |
    |:--------|:-----------------------------------|:------------------------------------------|
    | GET     | `/api/payment/{id}`                | Get payment details using payment ID      |
    | POST    | `/api/payment`                     | Initiate payment                          |
    | POST    | `/api/payment/{id}/refund`         | Initiate payment refund                   |





1. Client---> order
2. order ---> Inventory
3. order ---> Payment
4. order ---> client (Response failed or success)


1. Client ---> order
2. order  ---> client (Initiated)
3. order  ---> Inventory
4. Inventory ---> Payment
5. Payment ---> Order (update status)

### Improvements that can be done-
1. Converting to Event based Architecture
2. One order can have multiple Products