package com.jingsky.mv.config;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


@Component("AuthInterceptor")
public class AuthInterceptor extends HandlerInterceptorAdapter {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //临时书写防止接口外漏
        String token="9ecc5508661b46c1ae4e7bcd986cd847";
        String tokenPara=request.getParameter("token");
        return  token.equalsIgnoreCase(tokenPara);
    }
}
