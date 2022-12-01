const amqp = require('amqplib/callback_api');


amqp.connect(`amqp://localhost`,(err,connection) => {
    if(err){
        throw err;
    }
    connection.createChannel((err,channel) => {
        if(err){
            throw err;
        }
        let queueName = process.argv[2];
        // let queueName1 = process.argv[2]+"partition2";
        
       

        //console.log(3)
        topic=queueName;
        const f= require('fs');
        const readline=require('readline');
        var user_file='topics.txt';
        var r=readline.createInterface({
            input : f.createReadStream(user_file)
        });
        let partitions=0;
        r.on('line',function(text){
            text=text.split(",");
            //console.log(1)

            if(text[0]==queueName){
                partitions = text[1];
                partitions=parseInt(partitions);
                //console.log(partitions)
            }

        });
        for(let i=0;i<partitions;i++)
        {
            //console.log(4)
            i=i.toString();
            queueName=topic+"partition"+i;
            //queueName=queueName.concat(i);
            channel.assertQueue(queueName,{
                durable : false
            });
            channel.consume(queueName,(msg) =>{
                
                console.log("Received from partition : "+i)

                console.log(`Received : ${msg.content.toString()}`);
                channel.ack(msg);
            })
        }   





        // channel.assertQueue(queueName1,{
        //     durable : false
        // });
        // channel.consume(queueName1,(msg1) =>{
        //     console.log("Received from partition2 : ")
        //     console.log(`Received : ${msg1.content.toString()}`);
        //     channel.ack(msg1);
        // })

    })
})