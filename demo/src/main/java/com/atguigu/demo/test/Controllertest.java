package com.atguigu.demo.test;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controllertest {
    @RequestMapping("test")
   // @ResponseBody
    public String test(){
        System.out.println("aaa");
        return "abc";
    }
    @RequestMapping("test02")
    public String test02(@RequestParam("name")String name,
                         @RequestParam("age")int age){
        return name +": " +age;
    }
}
