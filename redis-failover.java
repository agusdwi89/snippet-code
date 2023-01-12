package com.telkomsel.digipos.redisfailover.service;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RedisService {
    RedisTemplate redisTemplate;
    RedisTemplate redisTemplateReplica;
    private RedisTemplate[] redisTemplates;
    private int redisIndex;
    public RedisService(RedisTemplate redisTemplate, RedisTemplate redisTemplateReplica) {
        this.redisTemplate = redisTemplate;
        this.redisTemplateReplica = redisTemplateReplica;
        redisTemplates = new RedisTemplate[]{redisTemplate, redisTemplateReplica};

    }

    @CircuitBreaker(name = "redisCircuitBreaker", fallbackMethod = "switchTemplate")
    public String storeData(String key, String value) {
        log.info("writing data to redis");
        redisTemplates[redisIndex].opsForValue().set(key, value);
        return (String) redisTemplates[redisIndex].opsForValue().get(key);
    }

    public String switchTemplate(String stringKey, String stringValue , Exception exception) throws InterruptedException {
        //  this gets called back with every exception but only  do the switch
        //  when it is called by the called not permitted exception (circuit breaker open)
        String exceptionMessage = "";
        if(exception == null) {
            exceptionMessage = "exception is Null";
        } else {
            exceptionMessage = exception.getMessage();
        }
        log.info("switchtemplate with exception " + exceptionMessage);
        boolean returnFailedOver = false;
        if ( exception == null || exception instanceof CallNotPermittedException) {
            // toggle the redis template to use to failover
            if (redisIndex == 0) {
                redisIndex = 1;
                log.info("Failed over from redistemplate1 to redistemplate2 redisIndex is " + redisIndex);
            } else {
                redisIndex=0;
                log.info("Failed over from redistemplate2 to redistemplate1 redisIndex is " + redisIndex);
            }
            returnFailedOver = true;
        }
        log.info("Failover is " + returnFailedOver + " redisIndex is " + redisIndex);
        return null;
    }
}
