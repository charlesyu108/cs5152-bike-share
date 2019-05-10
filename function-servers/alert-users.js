module.exports = async function (context, req) {
    context.log('Processing new request.');
    const http = require('http');
    const host = "52.224.137.86";
    const port = 80;
    //const url = "http://20.42.27.246:80/getPrediction"

    const options = {
        host: host,
        port: port,
        path: "/getPrediction",
        method: 'GET'
    }
    if (req.body){
        //TODO: Parse user info from event
        context.log('Got new message: ' + JSON.stringify(req.body));
        //TODO: Get latest prediction

        var promise1 = new Promise( (resolve, reject) => {
            var predict_req = http.request(options, (res) => {
                context.log(`statusCode: ${res.statusCode}`);

                res.on('data', (d) => {
                    context.log(`Response received!: ${d}`);
                    //TODO: Alert user
                    context.res = {
                        status: 200,
                        body: d
                    };
                    resolve(res);
                });
            });
            //context.log(predict_req);

            predict_req.on('error', (error) => {
                console.error(error);
                reject(error);
            });
            predict_req.end();
        });

        await promise1;
    }
    else {
        context.res = {
            status: 400,
            body: "No body in requests."
        }
    }
};