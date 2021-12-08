package com.jingsky.mv.controller;

import com.jingsky.mv.service.CommonService;
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
    private CommonService commonService;

    /**
     * 终止同步任务
     *
     * @return Response
     */
    @RequestMapping("terminate-transfer")
    public Response terminateTransfer() {
        commonService.terminateTransfer();
        return new Response();
    }

    /**
     * 启动任务，将先将表写入到bootstrap。
     *
     * @return Response
     */
    @RequestMapping("start-transfer")
    public Response startTransfer() throws Exception {
        commonService.startTransfer();
        return new Response();
    }

    /**
     * 恢复任务
     *
     * @return Response
     */
    @RequestMapping("resume-transfer")
    public Response resumeTransfer() {
        commonService.resumeTransfer();
        return new Response();
    }

    /**
     * 重置任务
     *
     * @return Response
     */
    @RequestMapping("reset-transfer")
    public Response resetTransfer() {
        commonService.resetTransfer();
        return new Response();
    }
}

