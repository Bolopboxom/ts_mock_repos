package com.airline.loyalty.interfaces.rest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/loyalty")
public class LoyaltyController {

    @PostMapping("/reserve")
    public ResponseEntity<String> reservePoints(@RequestBody String body) {
        return ResponseEntity.accepted().body("points reserved (mock)");
    }
}
