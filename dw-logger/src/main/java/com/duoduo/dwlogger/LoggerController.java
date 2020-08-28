package com.duoduo.dwlogger;

/**
 * Author z
 * Date 2020-08-25 09:44:43
 */
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LoggerController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    @Autowired
    KafkaTemplate kafkaTemplate;
    
    @RequestMapping("/applog")
    public String applog(@RequestBody JSONObject jsonObject){
        String logJson = jsonObject.toJSONString();
        logger.info(logJson);
        if(jsonObject.getString("start")!=null){
            kafkaTemplate.send("GMALL_START",logJson);
        }else{
           kafkaTemplate.send("GMALL_EVENT",logJson);
        
        }
        return logJson;
    }
}