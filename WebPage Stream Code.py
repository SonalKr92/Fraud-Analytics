var http = require("http");
var express = require('express');
var app = express();
var execPhp = require('exec-php');
var bodyParser = require('body-parser');
var urlencodedParser = bodyParser.urlencoded({ extended: true });
var path = require('path') 
// Running Server Details.
var server = app.listen(8082, function () {
  var host = server.address().address
  var port = server.address().port
  console.log("Example app listening at %s:%s Port", host, port)
});
 
 
app.get('/', function (req, res) {

  res.sendFile('/mnt/reference/data/Internship/WebPage/PaymentPage1.html');
});
app.use(express.static(path.join(__dirname, 'public'))); 
var cardNumberSent = ""
var timeStampSent = ""
app.post('/thank', urlencodedParser, function (req, res){
 
 var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    client = new kafka.Client('ip-10-0-1-25:2181'),
    producer = new Producer(client);

	
		/*Creating a payload, which takes below information
		'topic' 	-->	this is the topic we have created in kafka. (test)
		'messages' 	-->	data which needs to be sent to kafka. (JSON in our case)
		'partition' -->	which partition should we send the request to. (default)
						
						example command to create a topic in kafka: 
						[kafka@kafka kafka]$ bin/kafka-topics.sh \
									--create --zookeeper localhost:2181 \
									--replication-factor 1 \
									--partitions 1 \
									--topic test
						
						If there are multiple partition, then we optimize the code here,
						so that we send request to different partitions. */
				
	//var d = new Date();
    var d = new Date();
	var curr_date = d.getDate();
	var curr_month = d.getMonth();
	curr_month++;
	var curr_year = d.getFullYear();
	var curr_hour = d.getHours();
	var curr_min = d.getMinutes();
	var curr_sec = d.getSeconds();
	var curr_msec = d.getMilliseconds();
	var p = curr_year + "-" + curr_month + "-" + curr_date + " " + curr_hour + ":" + curr_min + ":" + curr_sec + "." + curr_msec;
	var n = d.getTime()
	timeStampSent = p;
	UniqueID = n
	var m = [req.body.No,req.body.amount,req.body.name,req.body.cvv,timeStampSent,UniqueID];
	cardNumberSent = req.body.No;
	//var m = ["hi", "hello", "you"];
	payloads = [
        { topic: 'fraudCase', messages: String(m), partition: 0 },
    ];
	
//	producer 'on' ready to send payload to kafka.
producer.on('ready', function(){
	producer.send(payloads, function(err, data){
		console.log(data)
	});
});

producer.on('error', function(err){})
 
  console.log("Wait for 10 seconds starts here .............. ")
  var sleep = require('system-sleep');
  sleep(10000); // 10 seconds 
  console.log("Wait for 10 seconds ends here.")
 
  // Kafka Consumer - Starts Here
  _predictedValue = "2";
  var cardNumberReceived = "";
  //var topicNameConsume = 'consumption'
  var topicNameConsume = 'fa_output'
  //var topics = [{ topic: topicName }];
  var topics = [{ topic: topicNameConsume }];
  var options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };
  
  var Consumer = kafka.Consumer;
  var Offset = kafka.Offset;
  var consumer = new Consumer(client, topics, options);
  var offset = new Offset(client);

  var consumedMessage = "";
  consumer.on('message', function (message) {
   console.log("Printing 1 &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ");
   console.log(message);
   console.log("Printing 2 ******************************************* ");
   console.log(message.value);
   console.log("Printing 3 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  ");
   console.log(typeof message.value);
   consumedMessageTemp = message.value
   if (consumedMessageTemp != "") {
   UniqueIDrec = consumedMessageTemp.split("|")[0].split(":")[1];
	_predictedValue = consumedMessageTemp.split("|")[1].split(":")[1];
	}
   consumedMessage = "Consumed message: " + message.value + "Predicted Value: " + _predictedValue;
   console.log(consumedMessage);
   console.log(_predictedValue);
   
     console.log("Printing 4: ");
  console.log(_predictedValue);
  var l = UniqueID.toString();
  
  try {  
  if(UniqueIDrec == l && _predictedValue=="0"){
  
  var html='';
  html +="<body>";
  html +="<h1> Transaction is successful.</h1>";
  html +="<img src='success.JPG' style='width:1350px;height:670px;'>";
  html +="</img>";
  html +="</body>";   
  res.send(html);   
  }
  else {
  var reply='';
     reply += "Transaction is failed. Denied: "+_predictedValue;
   return res.send(reply);
  }
  } catch (err_msg) {
    // handle the error safely
    console.log(err_msg)
 }
   console.log("Printing 5: ");
  }
  
  );

  console.log("Printing 6: ");
  
  consumer.on('error', function (err) {
   console.log('error', err);
  });
  // Kafka Consumer - Ends Here      
  
/*
execPhp('file1.php', function(error, php, outprint){
    php.my_function(req.body.No, req.body.amount, function(error, result){
        
    var s=JSON.parse(result);
    
    if(s.result.target=="0")
    {var html='';
  html +="<body>";
  html +="<img src='success.JPG' style='width:1350px;height:670px;'>";
  html +="</img>";
  html +="</body>";   
  
 
  res.send(html);   }
  else {var reply='';
 

  
    reply += "denied"+s.result.target;
  res.send(reply);} });
}); */
});