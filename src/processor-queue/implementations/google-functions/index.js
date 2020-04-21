const { PubSub } = require(`@google-cloud/pubsub`)
const grpc = require('grpc');
const { Storage } = require(`@google-cloud/storage`)
const fs = require(`fs-extra`)
const path = require(`path`)
const log = require(`loglevel`)
const { topicFor, bucketFor } = require(`./utils`)

const DEFAULT_MAX_PUB_SUB_SIZE = 1024 * 1024 * 5 // 5 Megabyte

class GoogleFunctions {
  constructor({
    processorSettings,
    maxPubSubSize,
    noSubscription,
    googleConfig,
  }) {
    const maxWaitTime = 30
    this.maxPubSubSize = maxPubSubSize || DEFAULT_MAX_PUB_SUB_SIZE
    const config =
      googleConfig ||
      JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))
    this.subName = `gatsby-sub-${Date.now()}-pull`
    this.workerBucket = bucketFor(processorSettings)
    this.workerTopic = topicFor(processorSettings)
    this.resultBucketName = `event-results-${process.env.TOPIC}`
    this.resultTopic = process.env.TOPIC
    this.pubSubClient = new PubSub({ projectId: config.project_id, grpc: grpc })
    this.storageClient = new Storage({ projectId: config.project_id })
    this.subscriptionDeadline = parseInt(process.env.DEADLINE_SECONDS, 10) || 200 
    this.maxInProgress = 50
    this.subscribers = []
    this.batchPublisher = this.pubSubClient.topic(this.workerTopic, {
      batching: {
        maxMessages: 50,
        maxMilliseconds: maxWaitTime * 1000,
      },
    });

    return (async () => {
      const topicCreatedFile = path.join(
        `.cache`,
        `topic-created-${process.env.TOPIC}`
      )
      const exists = await fs.pathExists(topicCreatedFile)
      if (exists) {
        return this
      }

      try {
        if (!noSubscription) {
          await this._createTopic()
          await this._createBucket()
        }
      } catch (err) {
        return Promise.reject(
          `Failed to start Google PubSub subscription: ${err}`
        )
      }

      await this._createSubscription()
      await fs.ensureFile(topicCreatedFile)

      return this
    })()
  }

  subscribe(handler) {
    this.subscribers.push(handler)
  }

  async publish(id, msg) {
    if (msg.byteLength < this.maxPubSubSize) {
      log.debug(`Publishing ${id} to pubsub ${this.workerTopic}`)
      await this.batchPublisher.publish(msg)
    } else {
      log.debug(`Publishing ${id} to storage ${this.workerBucket}`)
      await this.storageClient
        .bucket(this.workerBucket)
        .file(`event-${id}`)
        .save(msg.toString(`base64`), { resumable: false })
    }
  }

  async _messageHandler(msg) {
    msg.ack()
    const pubSubMessage = JSON.parse(Buffer.from(msg.data, `base64`).toString())
    if (pubSubMessage.storedPayload) {
      const payload = await this._downloadFromStorage(
        msg.id,
        pubSubMessage.storedPayload
      )
      pubSubMessage.payload = payload
      delete pubSubMessage.storedPayload
    }
    this.subscribers.forEach(handler => handler(pubSubMessage))
  }

  async _createTopic() {
    try {
      await this.pubSubClient.createTopic(this.resultTopic)
    } catch (err) {
      log.trace(`Create result topic failed`, err)
    }
  }

  async _createSubscription() {
    // Creates a new subscription
    const [subscription] = await this.pubSubClient
      .topic(this.resultTopic)
      .createSubscription(this.subName, { ackDeadlineSeconds: this.subscriptionDeadline, flowControl: {maxMessages: this.maxInProgress} })
    log.info('subscription created ---> ', this.subName)
    subscription.on(`message`, this._messageHandler.bind(this))
    subscription.on(`error`, err => log.error(`Error from subscription: `, err))
    subscription.on(`close`, err =>
      log.error(`Subscription closed unexpectedly`, err)
    )
  }

  async _createBucket() {
    try {
      const lifeCycle = `<?xml version="1.0" ?>
        <LifecycleConfiguration>
            <Rule>
                <Action>
                    <Delete/>
                </Action>
                <Condition>
                    <Age>30</Age>
                </Condition>
            </Rule>
        </LifecycleConfiguration>`
      const [bucket] = await this.storageClient.createBucket(
        this.resultBucketName
      )
      await bucket.setMetadata({ lifeCycle })
    } catch (err) {
      log.trace(`Create result bucket failed`, err)
    }
  }

  async _downloadFromStorage(id, storedPayload) {
    const file = this.storageClient
      .bucket(this.resultBucketName)
      .file(storedPayload)
    await file.download({ destination: `/tmp/result-${id}` })
    const data = (await fs.readFile(`/tmp/result-${id}`)).toString()
    const payload = JSON.parse(data)
    await fs.remove(`/tmp/result-${id}`)
    return payload
  }
}

exports.GoogleFunctions = GoogleFunctions
