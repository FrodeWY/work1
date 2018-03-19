package com.service;

import com.rabbitmq.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RabbitMQService {
    private String queueName="fanout_queue";
    @Autowired
    private ConnectionFactory connectionFactory;
    public void receive() throws IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
      /*创建一个已经生成名字的、排他性的且会自动删除的Queue：*/
//        String queueName = channel.queueDeclare().getQueue();
        channel.queueDeclare("fanout_queue1",true,false,false,null);
        channel.queueBind("fanout_queue1","fanout_exchange","");
        /**使用basicQos放来来设置消费者最多会同时接收多少个消息。这里设置为1，表示RabbitMQ同一时间发给消费者的消息不超过一条。这样就能保证消费者在处理完某个任务，
         并发送确认信息后，RabbitMQ才会向它推送新的消息，在此之间若是有新的消息话，将会被推送到其它消费者，若所有的消费者都在处理任务，那么就会等待。*/
        //指定该消费者同时只接收一条消息
//        channel.basicQos(1);
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long delivery = envelope.getDeliveryTag();
                String message =new String(body,"UTF-8");
                System.out.println("message:"+message);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (message.equals("hello")) {
                    channel.basicAck(delivery, true);
                } else if(message.equals("hello2")) {
//                    channel.basicAck(delivery,false);
                    /*channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    deliveryTag:该消息的index
                    multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息。
                    requeue：被拒绝的是否重新入队列*/
                    channel.basicNack(delivery,true,false);
                }else {
                    /**deliveryTag:该消息的index
                     multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。*/
                    channel.basicAck(delivery,false);
                }

            }
        };
        /*autoAck：是否自动ack，如果不自动ack，需要使用channel.ack、channel.nack、channel.basicReject 进行消息应答*/
        channel.basicConsume("fanout_queue1", false,"myConsumerTag",defaultConsumer);

    }

    public void receive2() throws IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        AMQP.Queue.DeclareOk fanout_queue2 = channel.queueDeclare("fanout_queue2", true, false, false, null);
        channel.queueBind("fanout_queue2","fanout_exchange","");

//        channel.basicQos(1);
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long delivery = envelope.getDeliveryTag();
                String message =new String(body,"UTF-8");
                System.out.println("message2:"+message);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (message.equals("hello")) {
                    channel.basicAck(delivery, true);
                } else if(message.equals("hello2")) {
//                    channel.basicAck(delivery,false);
                    /*channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    deliveryTag:该消息的index
                    multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息。
                    requeue：被拒绝的是否重新入队列*/
                    channel.basicNack(delivery,true,false);
                }else {
                    /**deliveryTag:该消息的index
                     multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。*/
                    channel.basicAck(delivery,false);
                }

            }
        };
        /*autoAck：是否自动ack，如果不自动ack，需要使用channel.ack、channel.nack、channel.basicReject 进行消息应答*/
        channel.basicConsume("fanout_queue2", false,"myConsumerTag",defaultConsumer);


    }

    public void receive3() throws IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare("fanout_queue3",true,false,false,null);
        channel.queueBind("fanout_queue3","fanout_exchange","");
//        channel.basicQos(1);
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String routingKey = envelope.getRoutingKey();
                String contentType = properties.getContentType();
                long delivery = envelope.getDeliveryTag();
                String message =new String(body,"UTF-8");
                System.out.println("message3:"+message);
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (message.equals("hello")) {
                    channel.basicAck(delivery, true);
                } else if(message.equals("hello2")) {
//                    channel.basicAck(delivery,false);
                    /*channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                    deliveryTag:该消息的index
                    multiple：是否批量.true:将一次性拒绝所有小于deliveryTag的消息。
                    requeue：被拒绝的是否重新入队列*/
                    channel.basicNack(delivery,true,false);
                }else {
                    /**deliveryTag:该消息的index
                     multiple：是否批量.true:将一次性ack所有小于deliveryTag的消息。*/
                    channel.basicAck(delivery,false);
                }

            }
        };
        /*autoAck：是否自动ack，如果不自动ack，需要使用channel.ack、channel.nack、channel.basicReject 进行消息应答*/
        channel.basicConsume("fanout_queue3", false,"myConsumerTag",defaultConsumer);

    }


    public void receive4() throws IOException, TimeoutException {
        Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare("direct_get",true,false,false,null);
        channel.queueBind("direct_get","direct_exchange","get_key");
        GetResponse getResponse = channel.basicGet("direct_get", true);
        if(getResponse==null){
            return;
        }
        byte[] body = getResponse.getBody();
        String messge=new String (body,"UTF-8");
        long deliveryTag = getResponse.getEnvelope().getDeliveryTag();
        String contentType = getResponse.getProps().getContentType();
        System.out.println("message:"+messge+"  deliveryTag:"+deliveryTag+"  contentType:"+contentType);

    }
}
