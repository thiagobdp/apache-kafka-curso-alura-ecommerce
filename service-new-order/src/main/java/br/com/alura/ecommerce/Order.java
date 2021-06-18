package br.com.alura.ecommerce;

import java.math.BigDecimal;

/**
 * utilizada apenas para serializar e deserializar as mensagens
 *
 */
public class Order {

    private final String orderId;
    private final BigDecimal amount;
    private final String email;

    public Order(String orderId, BigDecimal amount, String email) {
        this.orderId = orderId;
        this.amount = amount;
        this.email = email;
    }
}
