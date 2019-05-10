module.exports = function (context, IoTHubMessages) {
    context.log(`JavaScript eventhub trigger function called for message array: ${IoTHubMessages}`);
    const http = require('http');
    const hostname = "alert-users.azurewebsites.net";
    const path = "/api/HttpTrigger1?code=klS8lGyqgCEYtkZG5YhU27lUgAcIuhTG3VCgnhFoSIXINO5PotBVyA==";

    var batchList = [];

    IoTHubMessages.forEach(message => {
        context.log(`Processed message: ${message}`);

        var payload = JSON.stringify(message);
        //var payload = message;
        context.log(payload);
        // push to list to be sent to storage queue
        batchList.push(payload);

        const options = {
            hostname: hostname,
            path: path,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(payload)
            }
        }
        
        const req = http.request(options, (res) => {
            context.log(`statusCode: ${res.statusCode}`);

            res.on('data', (d) => {
                context.log(`Response received!: ${d}`);
            });
        });

        req.on('error', (error) => {
            console.error(error);
        });

        req.write(payload);
        req.end();
    });

    var output = {
        "messages": batchList
    };

    context.log(`batch list: ${batchList}`);
    context.bindings.bikeEvent = output;
    
    // add to storage queue, make sure to uncomment later
    context.bindings.batchQueueItem = batchList;

    context.done();
};