const amqp = require('amqplib/callback_api');


amqp.connect(`amqp://localhost`,(err,connection) => {
    if(err){
        throw err;
    }
    connection.createChannel((err,channel) => {
        if(err){
            throw err;
        }
        let queueName0 = process.argv[2]+"partition0";
        let queueName1 = process.argv[2]+"partition1";
        let queueName2 = process.argv[2]+"partition2";
        let queueName3 = process.argv[2]+"partition3";
        let queueName4 = process.argv[2]+"partition4";
        let queueName5 = process.argv[2]+"partition5";
        let queueName6 = process.argv[2]+"partition6";
        let queueName7 = process.argv[2]+"partition7";
        let queueName8 = process.argv[2]+"partition8";
        let queueName9 = process.argv[2]+"partition9";

        
        // const f= require('fs');
        // const readline=require('readline');
        // var user_file='topics.txt';
        // var r=readline.createInterface({
        //     input : f.createReadStream(user_file)
        // });
        // r.on('line',function(text){
        //     text=text.split(",");
        //     if(text[0]==queueName){
        //         partitions = text[1];
        //     }

        // });
      
        channel.assertQueue(queueName0,{
            durable : false
        });
        channel.consume(queueName0,(msg) =>{
            console.log("Received from partition0: ")
            console.log(`Received : ${msg.content.toString()}`);
            channel.ack(msg);
        })
        channel.assertQueue(queueName1,{
            durable : false
        });
        channel.consume(queueName1,(msg1) =>{
            console.log("Received from partition1: ")
            console.log(`Received : ${msg1.content.toString()}`);
            channel.ack(msg1);
        })
        channel.assertQueue(queueName2,{
            durable : false
        });
        channel.consume(queueName2,(msg2) =>{
            console.log("Received from partition2 : ")
            console.log(`Received : ${msg2.content.toString()}`);
            channel.ack(msg2);
        })
        channel.assertQueue(queueName3,{
            durable : false
        });
        channel.consume(queueName3,(msg3) =>{
            console.log("Received from partition3 : ")
            console.log(`Received : ${msg3.content.toString()}`);
            channel.ack(msg3);
        })
        channel.assertQueue(queueName4,{
            durable : false
        });
        channel.consume(queueName4,(msg4) =>{
            console.log("Received from partition4 : ")
            console.log(`Received : ${msg4.content.toString()}`);
            channel.ack(msg4);
        })
        channel.assertQueue(queueName5,{
            durable : false
        });
        channel.consume(queueName5,(msg5) =>{
            console.log("Received from partition5 : ")
            console.log(`Received : ${msg5.content.toString()}`);
            channel.ack(msg5);
        })
        channel.assertQueue(queueName6,{
            durable : false
        });
        channel.consume(queueName6,(msg6) =>{
            console.log("Received from partition6 : ")
            console.log(`Received : ${msg6.content.toString()}`);
            channel.ack(msg6);
        })
        channel.assertQueue(queueName7,{
            durable : false
        });
        channel.consume(queueName7,(msg7) =>{
            console.log("Received from partition7 : ")
            console.log(`Received : ${msg7.content.toString()}`);
            channel.ack(msg7);
        })
        channel.assertQueue(queueName8,{
            durable : false
        });
        channel.consume(queueName8,(msg8) =>{
            console.log("Received from partition8 : ")
            console.log(`Received : ${msg8.content.toString()}`);
            channel.ack(msg8);
        })
        channel.assertQueue(queueName9,{
            durable : false
        });
        channel.consume(queueName9,(msg9) =>{
            console.log("Received from partition9 : ")
            console.log(`Received : ${msg9.content.toString()}`);
            channel.ack(msg9);
        })

    })
})