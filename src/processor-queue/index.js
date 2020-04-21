"use strict"
const { Queue, Job } = require(`./queue`)
const log = require(`loglevel`)

const DEFAULT_MAX_MESSAGE_MEM = 1024 * 1024 * 20 * 10 // 2000 megabytes

const sleep = (ms) => {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const until = async (fn) => {
  let waitTime = 0
  while (!fn()) {
      waitTime += 2000
      await sleep(2000)
  }
  return waitTime
}

class ProcessorQueue {
  constructor({ maxJobTime, maxMessageMem, pubSubImplementation }) {
    this._mem = 0
    this.maxMessageMem = maxMessageMem || DEFAULT_MAX_MESSAGE_MEM

    this.queue = new Queue({ maxJobTime, maxMessageMem, pubSubImplementation })
  }

  async process(payload) {
    let size = 0
    try {
      const job = await new Job(payload)
      size = job.fileSize
      //await this._waitForFreeMessageMem()
      const waitTime = await until(() => this._mem <= this.maxMessageMem)
      if(waitTime > 0){
        log.info(`max mem throttling ${waitTime / 1000} seconds`)
      }
      this._mem += size
      const msg = await job.msg()
      const result = await this.queue.push(job.id, msg)
      this._mem -= size
      return result
    } catch (err) {
      this._mem -= size
      return Promise.reject(err)
    }
  }

  async _waitForFreeMessageMem() {
    return new Promise((resolve, reject) => {
      const check = () => {
        if (this._mem <= this.maxMessageMem) {
          return resolve()
        }
        return setTimeout(check, 100)
      }
      check()
    })
  }
}

exports.ProcessorQueue = ProcessorQueue
