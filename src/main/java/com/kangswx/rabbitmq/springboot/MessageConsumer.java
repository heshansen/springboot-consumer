package com.kangswx.rabbitmq.springboot;

import com.kangswx.rabbitmq.springboot.domain.Employee;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;

@Component
public class MessageConsumer {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    //用于接收消息的方法
    //RabbitListener用于声明定义消息接收的队列与exchange绑定的信息，使消费者一直处于等待状态
    //在SpringBoot中消费者这端使用注解获取消息
    @RabbitListener(
        bindings = @QueueBinding(
                value = @Queue(value = "springboot-queue", durable = "true"),  //队列的信息，当队列不存在的时候，会自动创建一个队列，并绑定下面的交换机
                exchange = @Exchange(value = "springboot-exchange", durable = "true", type = "topic"), //交换机的信息
                key = "#"  //路由规则
        )
    )
    //RabbitHandler注解，通知SpringBoot该方法用于接收消息,这个方法运行后将处于等待状态，有新的消息进来就会自动触发该方法处理消息
    @RabbitHandler
    public void handleMessage(@Payload Employee employee, Channel channel,  //Payload将接收的消息反序列化后注入到Employee对象
                              @Headers Map<String, Object> headers){  //headers获取辅助描述信息
        System.out.println("=====================");
        System.out.println("empno: "+employee.getEmpno() + ", name:" + employee.getName() + ", age:" + employee.getAge());

        long tag = (long) headers.get(AmqpHeaders.DELIVERY_TAG);
        try {
            //第一个参数，tag
            //第二个参数，是否批量接收
            channel.basicAck(tag, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("=====================");
    }
}
