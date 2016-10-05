/*
监听端口  3000
*/
var net = require('net');
var mongodb=require("mongodb")
var redis =require("redis");

var redisClient;
var mongodbServer;
var mongodbClient;
var mongo;

function DBInit()
{
    mongodbServer = new mongodb.Server('localhost', 27017, { auto_reconnect: true, poolSize: 10 });
    mongodbClient = new mongodb.Db('uploaddb', mongodbServer);

    mongodbClient.open(function(err, db) {
        if(!err) {
            mongo=db;
        }
    });

    redisClient = redis.createClient();
    redisClient.on("error", function (err) {
        console.log("Error " + err);
    });
}
DBInit();

tcpserver=net.createServer(function(socket) {
    var messagCount=0;//第一次发数据需要得到模块唯一ID
    var uploadData;  //上传的数据
    var SN;  //序列号

    socket.setNoDelay(true);//不使用延时
    var res={error:0,msg:"connected"};//连接成功反馈
    var resStr=JSON.stringify(res);
    socket.write(resStr);

    socket.on('data', function(data) {
        uploadData=data.toString();
        if(messagCount===0)   //注册设备
        {
            messagCount=1;

            SN=uploadData;//  第一次序列号
            if(!SN.startwith("SN"))
            {
                var res={error:5,msg:"registerErr"};
                var resStr=JSON.stringify(res);
                socket.write(resStr);
                return;
            }

            var devInfo={};//需要存储到REDIS
            devInfo.ip=socket.remoteAddress;//获取IP地址,地理位置定位用
            devInfo.time= Date.parse(new Date());

            redisClient.hset("device","info_"+SN, JSON.stringify(devInfo), function(err, data){   //超时事件
               if(err) {
                   var res={error:1,msg:"dberr"};
                   var resStr=JSON.stringify(res);
                   socket.write(resStr);
               } else {
                   var res={error:0,msg:"registered"};
                   var resStr=JSON.stringify(res);
                   socket.write(resStr);
               }
            })
         }else
        {//上传数据
            redisClient.hset("device","data_"+SN,uploadData, function(err, data){   //超时事件
                if(err) {
                    var res={error:1,msg:"dberr"};
                    var resStr=JSON.stringify(res);
                    socket.write(resStr);
                } else {
                    redisClient.hincrby('device', "count_"+SN, 1,function(err, data){
                        if(err) {
                            console.log(err);
                        }else
                        {
                            mongo.collection("DB_"+SN, function(err, collection) {
                                if (err) {

                                }else
                                {
                                    try{
                                        var json = JSON.parse(uploadData);
                                        collection.insert(json, function (err, data) {
                                            if (err) {
                                                var res={error:2,msg:"dberr"};
                                                var resStr=JSON.stringify(res);
                                                socket.write(resStr);
                                            }
                                        });
                                        console.log("mongodb insert");
                                    }catch(e){
                                        var res={error:4,msg:"daterr"};
                                        var resStr=JSON.stringify(res);
                                        socket.write(resStr);
                                    }
                                }
                            });
                        }
                    });
                }
            })
        }
   });
    //60秒没数据  就结束
     socket.setTimeout(60000,function(){
        var res={error:3,msg:"timeout"};
        var resStr=JSON.stringify(res);
        socket.write(resStr);
        socket.end();
     })
    //连接结束
    socket.on('end', function(data) {
        //销毁变量
        delete messagCount;
    });
}).listen(3000);

tcpserver.on('listening', function() {
 console.log('Server is listening on port');
 });

