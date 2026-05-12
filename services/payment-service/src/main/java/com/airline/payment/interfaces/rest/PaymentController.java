package com.airline.payment.interfaces.rest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    @PostMapping("/charge")
    public ResponseEntity<String> charge(@RequestBody String body) {
        return ResponseEntity.accepted().body("payment processing (mock)");
    }
}
