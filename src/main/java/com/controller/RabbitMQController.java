package com.controller;

import com.service.RabbitMQService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@RestController
public class RabbitMQController {
    @Autowired
    private RabbitMQService rabbitMQService;
    @GetMapping("work1")
    public void work1() throws IOException, TimeoutException {
        rabbitMQService.receive();
    }

    @GetMapping("work2")
    public void work2() throws IOException, TimeoutException {
        rabbitMQService.receive2();
    }

    @GetMapping("work3")
    public void work3() throws IOException, TimeoutException {
        rabbitMQService.receive3();
    }
    @GetMapping("work4")
    public void work4 () throws IOException, TimeoutException {
        rabbitMQService.receive4();
    }
}
