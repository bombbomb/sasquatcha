
var sinon               = require('sinon'),
    async               = require('async'),
    proxyquire          = require('proxyquire'),
    assert              = require('assert');

var httpsStubs = {
    get: sinon.spy()
};

var Sasquatcha          = proxyquire('../index',{ 'https': httpsStubs });

var saq = null,
    fireOnMessageTestEvent = null,
    fireOnMessageErrorTestEvent = null,
    dynamoPutItemSpy = sinon.spy();

function generateMockEvent(type)
{
    type = type || 'Notification';
    var messageData = {
        MessageId: "0275c5f1-d0d6-4169-b7ac-beff02f45bf6",
        Type: type,
        Token:"abc123",
        TopicArn:"arn:aws:sns:us-east-1:00000000000:Test",
        Message:"You have chosen to subscribe to the topic arn:aws:sns:us-east-1:00000000000:Test.\nTo confirm the subscription, visit the SubscribeURL included in this message.",
        SubscribeURL:"https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=arn:aws:sns:us-east-1:00000000000:Test&Token=abc123",
        Timestamp:"2017-05-18T17:43:55.873Z",
        SignatureVersion:"1",
        Signature:"cheers",
        SigningCertURL:"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem"
    };
    return {
        message: messageData,
        data: messageData,
        changeMessageVisibility: function(time,callback){
            callback(null,{});
        },
        deleteMessage: function(callback){
            callback(null,{});
        },
        next: function(){}
    }
}

// overrides/mocks for tests
Sasquatcha.prototype.getDynamo = function(){
    return {
        describeTable: function (params,callback) {
            callback(null,'good table');
        },
        putItem: function(params,callback) {
            dynamoPutItemSpy();
            callback(null,{});
        }
    };
};

Sasquatcha.prototype.getSQSQueue = function(){
    return {
        on: function (eventName,callback) {
            if (eventName === "message")
            {
                fireOnMessageTestEvent = callback;
            }
            if (eventName === "error")
            {
                fireOnMessageErrorTestEvent = callback;
            }
        }
    };
};


describe('method logic tests', function(){

    before(function(done){
        saq = new Sasquatcha({
            autoConfirmSubscriptions: false
        });
        setTimeout(done,500);
    });

    it('add watched queue',function(done){
        saq.addWatch('testQueueWatch', null, true, function(){
            done();
        });
        assert(dynamoPutItemSpy.called, 'Dynamo putItem was not called');
    });

    it('watch queue WITH auto-confirm',function(done){

        var testQueueEvent;
        sinon.spy(saq, "autoConfirmSubscription");
        sinon.spy(saq, "log");

        async.waterfall([function(callback){
            saq.watchQueue({ autoConfirm: true, queueName: 'testQueueAutoConfirmSubscription' },function(err, queueData, event, complete){
                complete(null);
            });
            callback(null);
        },function(callback){

            testQueueEvent = generateMockEvent('SubscriptionConfirmation');

            fireOnMessageTestEvent(testQueueEvent);

            callback(null);

        },function(callback){

            assert(saq.autoConfirmSubscription.called,'Expected autoConfirmSubscription to be called');
            assert(httpsStubs.get.calledWith(testQueueEvent.message.SubscribeURL),'Expected https.get to be called');
            assert(saq.log.called,'Expected log to be called');
            saq.log.restore();
            saq.autoConfirmSubscription.restore();
            callback(null);

        }],done);

    });

    it('watch queue WITHOUT auto-confirm',function(done){
        sinon.spy(saq, "autoConfirmSubscription");

        async.waterfall([function(callback){
            saq.watchQueue({ autoConfirm: false, queueName: 'testQueueAutoConfirmSubscription' },function(err, queueData, event, complete){
                complete(null);
            });
            callback(null);
        },function(callback){

            var testQueueEvent = generateMockEvent('SubscriptionConfirmation');

            fireOnMessageTestEvent(testQueueEvent);

            callback(null);

        },function(callback){

            assert(saq.autoConfirmSubscription.called === false,'Expected autoConfirmSubscription not to be called');
            saq.autoConfirmSubscription.restore();
            callback(null);

        }],done);

    });

    it('watch queue callback called',function(done){

        var callbackSpy = sinon.spy();
        async.waterfall([function(callback){
            saq.watchQueue({ queueName: 'testQueueEvents' },function(err, queueData, event, complete){
                callbackSpy();
                complete(null);
            });
            callback(null);
        },function(callback){

            var testQueueEvent = generateMockEvent();

            fireOnMessageTestEvent(testQueueEvent);

            callback(null);

        },function(callback){

            assert(callbackSpy.called,'Expected callbackSpy to be called');
            callback(null);

        }],done);

    });

});