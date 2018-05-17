'use strict';

const _ = require('underscore');
const Promise = require('bluebird');
const kafka = require('kafka-node');
const signalExit = require('signal-exit');

Promise.promisifyAll(kafka.ConsumerGroup.prototype);
Promise.promisifyAll(kafka.HighLevelProducer.prototype);

/**
 * @todo add producer / consumer profiles (different options sets)
 */
class KafkaClient {
    constructor() {
        this.producers = [];
        this.consumers = [];

        signalExit(() => this._close().then(() => process.exit()));
    }

    createConsumer(topicNames, options = {}) {
        KafkaClient.validateOptions(options);

        const onError = options.onError || KafkaClient.defaultErrorHandler;

        const consumerGroupOptions = {
            groupId: options.clientId,
            host: options.zookeeperHosts.join(','),
            zk: options.zookeeperOptions,
            autoCommit: false
        };

        const consumerGroup = new kafka.ConsumerGroup(consumerGroupOptions, topicNames);

        consumerGroup.on('error', onError);

        this.consumers.push(consumerGroup);

        return consumerGroup;
    }

    createProducer(options = {}) {
        KafkaClient.validateOptions(options);

        const onError = options.onError || KafkaClient.defaultErrorHandler;

        const client =
            new kafka.Client(
                options.zookeeperHosts.join(','),
                options.clientId,
                options.zookeeperOptions,
                options.noAckBatchOptions);

        const highLevelProducer = new kafka.HighLevelProducer(client, options.producer);

        highLevelProducer.on('error', onError);

        this.producers.push(highLevelProducer);

        return highLevelProducer;
    }

    _close() {
        const closeables = [...this.producers, ...this.consumers];

        if (closeables.length === 0)
            return;

        return Promise
            .map(closeables, closeable =>
                closeable
                    .closeAsync()
                    .catch(err => console.error('Close kafka object', err.stack || err)))
            .finally(() => {
                console.log(`Closed ${closeables.length} kafka objects`);
            });
    }

    static defaultErrorHandler(err) {
        console.error(err.stack || err);
    }

    static validateOptions(options) {
        if (!options) {
            throw new Error('Options are empty');
        }

        if (_.isEmpty(options.zookeeperHosts)) {
            throw new Error('Zookeeper host list is empty');
        }
    }
}

module.exports = new KafkaClient();