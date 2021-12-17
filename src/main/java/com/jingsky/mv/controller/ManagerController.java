package com.jingsky.mv.controller;

import com.jingsky.mv.service.ConfigService;
import com.jingsky.mv.service.JobService;
import com.jingsky.mv.util.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 同步管理Controller
 */
@RestController()
@RequestMapping("/manager")
public class ManagerController {
    @Autowired
    private JobService jobService;
    @Autowired
    private ConfigService configService;

    /**
     * 打印传输的信息
     * @return
     * @throws Exception
     */
    @RequestMapping("configInfo")
    public Response configInfo() throws Exception {
        return new Response(configService.getAllView());
    }

    /**
     * 终止同步任务
     *
     * @return Response
     */
    @RequestMapping("terminate-transfer")
    public Response terminateTransfer() {
        jobService.setTerminate(true);
        return new Response();
    }

    /**
     * 启动任务，将先将表写入到bootstrap。
     *
     * @return Response
     */
    @RequestMapping("start-transfer")
    public Response startTransfer() throws Exception {
        jobService.startTransfer();
        return new Response();
    }

    /**
     * 恢复任务
     *
     * @return Response
     */
    @RequestMapping("resume-transfer")
    public Response resumeTransfer() {
        jobService.setTerminate(false);
        return new Response();
    }
}

