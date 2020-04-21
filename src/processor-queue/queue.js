const fs = require(`fs-extra`)
const log = require(`loglevel`)

const DEFAULT_MAX_JOB_TIME = process.env.PARALLEL_RUNNER_TIMEOUT
  ? parseInt(process.env.PARALLEL_RUNNER_TIMEOUT, 10)
  : 5 * 60 * 1000

const MESSAGE_TYPES = {
  JOB_COMPLETED: `JOB_COMPLETED`,
  JOB_FAILED: `JOB_FAILED`,
}

const sleep = (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const until = async (fn) => {
  waitTime = 0
  while (!fn()) {
      waitTime += 2000
      await sleep(2000)
  }
  return waitTime
}


class Job {
  constructor({ id, args, file }) {
    this.id = id
    this.args = args
    this.file = file

    return (async () => {
      try {
        await this._calculateSize()
      } catch (err) {
        return Promise.reject(err)
      }
      return this
    })()
  }

  async msg() {
    const data = await this._readData()
    return Buffer.from(
      JSON.stringify({
        id: this.id,
        file: data.toString(`base64`),
        action: this.args,
        topic: process.env.TOPIC,
      })
    )
  }

  async _calculateSize() {
    if (this.file instanceof Buffer) {
      return (this.fileSize = this.file.byteLength)
    }
    try {
      const stat = await fs.stat(this.file)
      return (this.fileSize = stat.size)
    } catch (err) {
      return Promise.reject(err)
    }
  }

  async _readData() {
    if (this.file instanceof Buffer) {
      return this.file
    }
    return await fs.readFile(this.file)
  }
}

class Queue {
  constructor({ maxJobTime, pubSubImplementation }) {
    log.info('<<<<< QUEUE INITIATED >>>>>>')
    this._jobs = new Map()
    this.jobCount = 0
    this.completeJobCount = 0
    this.maxJobTime = maxJobTime || DEFAULT_MAX_JOB_TIME
    this.maxJobQueued = 600
    //this.queueWaitTime = 0
    this.pubSubImplementation = pubSubImplementation
    if (pubSubImplementation) {
      pubSubImplementation.subscribe(this._onMessage.bind(this))
    }
  }

  // async _waitForQueueMessages() {
  //   return new Promise((resolve, reject) => {
  //     const check = () => {
  //       if (this._jobs.size <= this.maxJobQueued) {
  //         if(this.queueWaitTime > 0){
  //           log.info(`throttling wait for ${this.queueWaitTime / 1000} seconds`)
  //           this.queueWaitTime = 0
  //         }
  //         return resolve()
  //       }
  //       this.queueWaitTime += 1000
  //       log.info(this.queueWaitTime)
  //       return setTimeout(check, 1000)
  //     }
  //     check()
  //   })
  // }

  async push(id, msg) {
    return new Promise(async (resolve, reject) => {
      this.jobCount += 1
      //await this._waitForQueueMessages()
      const waitTime = await until(() => this._jobs.size <= this.maxJobQueued)
      if(waitTime > 0){
        log.info(`throttled sleep for ${waitTime / 1000} seconds`)
      }
      this._jobs.set(id, { resolve, reject })
      setTimeout(() => {
        if (this._jobs.has(id)) {
          reject(`Job timed out ${id}`)
        }
      }, this.maxJobTime)
      try {
        await this.pubSubImplementation.publish(id, msg)
      } catch (err) {
        reject(err)
      }
    })
  }

  _onMessage(pubSubMessage) {
    const { type, payload } = pubSubMessage
    log.debug(`Got worker message`, type, payload && payload.id)

    switch (type) {
      case MESSAGE_TYPES.JOB_COMPLETED:
        if (this._jobs.has(payload.id)) {
          this._jobs.get(payload.id).resolve(payload)
          this.completeJobCount += 1
          this._jobs.delete(payload.id)
        }
        log.info(`queued jobs - ${this._jobs.size} completed jobs - ${this.completeJobCount} total jobs - ${this.jobCount}`)
        return
      case MESSAGE_TYPES.JOB_FAILED:
        log.info('JOB FAILED')
        if (this._jobs.has(payload.id)) {
          this._jobs.get(payload.id).reject(payload.error)
          this._jobs.delete(payload.id)
        }
        return
      default:
        log.error(`Unkown worker message: `, pubSubMessage)
    }
  }
}

exports.Job = Job
exports.Queue = Queue
