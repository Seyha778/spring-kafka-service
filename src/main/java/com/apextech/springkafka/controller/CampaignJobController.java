package com.apextech.springkafka.controller;

import com.apextech.springkafka.dto.response.ResponseMessage;
import com.apextech.springkafka.dto.response.ResponseMessageBuilder;
import com.apextech.springkafka.kafka.CampaignOnboardScheduler;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/campaign")
@RequiredArgsConstructor
public class CampaignJobController {

    private final CampaignOnboardScheduler campaignOnboardScheduler;

    @PostMapping("/onboard/run")
    public ResponseMessage<Object> triggerRun() {
        campaignOnboardScheduler.runDailyCampaign();
        return new ResponseMessageBuilder<>().success().build();
    }
}
