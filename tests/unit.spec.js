
var sinon               = require('sinon'),
    async               = require('async'),
    assert              = require('assert');

var Sasquatcha          = require('../index');

var saq = null,
    fireOnMessageTestEvent = null,
    fireOnMessageErrorTestEvent = null,
    dynamoPutItemSpy = sinon.spy();

function generateMockEvent()
{
    return {
        message: {
            MessageId: ''
        },
        data: {

        },
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

        sinon.spy(saq, "autoConfirmSubscription");

        async.waterfall([function(callback){
            saq.watchQueue({ autoConfirm: true, queueName: 'testQueueAutoConfirmSubscription' },function(err, queueData, event, complete){
                complete(null);
            });
            callback(null);
        },function(callback){

            var testQueueEvent = generateMockEvent();
            testQueueEvent.message.SubscribeURL = 'https://tests';

            fireOnMessageTestEvent(testQueueEvent);

            callback(null);

        },function(callback){

            assert(saq.autoConfirmSubscription.called,'Expected autoConfirmSubscription to be called');
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

            var testQueueEvent = generateMockEvent();
            testQueueEvent.message.SubscribeURL = 'https://tests';

            fireOnMessageTestEvent(testQueueEvent);

            callback(null);

        },function(callback){

            assert(saq.autoConfirmSubscription.called === false,'Expected autoConfirmSubscription not to be called');
            saq.autoConfirmSubscription.restore();
            callback(null);

        }],done);

    });

});