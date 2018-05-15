package com.jdriven.kafkaworkshop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;

import static com.jdriven.kafkaworkshop.TopicNames.RECEIVED_SENSOR_DATA;

@Controller
public class SensorController {

    private static final Logger LOGGER = LoggerFactory.getLogger(SensorController.class);
    private KafkaTemplate<String, SensorData> kafkaTemplate;

    public SensorController(final KafkaTemplate<String, SensorData> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/sensor")
    public String greetingForm(Model model) {
        model.addAttribute("sensorData", new SensorData());
        return "sensor";
    }

    @PostMapping("/sensor")
    public String sensorSubmit(@ModelAttribute SensorData sensorData) {
        kafkaTemplate.send(RECEIVED_SENSOR_DATA, sensorData.getId(), sensorData);
        return "sensor";
    }

}
