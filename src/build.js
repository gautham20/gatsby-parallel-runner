#!/usr/bin/env node

const cp = require(`child_process`)
const log = require(`loglevel`)
const path = require(`path`)
const fs = require(`fs-extra`)
const { ProcessorQueue } = require(`./processor-queue`)
const {
  GoogleFunctions,
} = require(`./processor-queue/implementations/google-functions`)
const { resolveProcessors } = require(`./utils`)

const MESSAGE_TYPES = {
  LOG_ACTION: `LOG_ACTION`,
  JOB_CREATED: `JOB_CREATED`,
  JOB_COMPLETED: `JOB_COMPLETED`,
  JOB_FAILED: `JOB_FAILED`,
  ACTIVITY_START: `ACTIVITY_START`,
  ACTIVITY_END: `ACTIVITY_END`,
  ACTIVITY_SUCCESS: `ACTIVITY_SUCCESS`,
  ACTIVITY_ERROR: `ACTIVITY_ERROR`,
}

function messageHandler(gatsbyProcess, processors = {}) {
  return async function(msg) {
    if (
      log.getLevel() <= log.levels.TRACE &&
      msg.type !== MESSAGE_TYPES.LOG_ACTION
    ) {
      log.trace(`Got gatsby message`, JSON.stringify(msg))
    }
    switch (msg.type) {
      case MESSAGE_TYPES.JOB_CREATED: {
        const processor = processors[msg.payload.name]
        if (!processor) {
          gatsbyProcess.send({
            type: `JOB_NOT_WHITELISTED`,
            payload: { id: msg.payload.id },
          })
          return
        }
        try {
          log.info("INCOMING MESSAGE", JSON.stringify(msg.payload))
          const outputDir = msg.payload.outputDir
          let toProcess = false
          let constructedResult = null
          if(fs.existsSync(outputDir)){
            const existingFiles = msg.payload.args.operations.filter((op) => {
              return fs.existsSync(path.join(outputDir, op.outputPath))
            })
            if(existingFiles.length == msg.payload.args.operations.length){
              toProcess = false
            }
          }
          if(toProcess){
            const result = await processor.process(msg.payload)
            log.info("RESULT", JSON.stringify(result))
            gatsbyProcess.send({
              type: `JOB_COMPLETED`,
              payload: {
                id: msg.payload.id,
                result,
              },
            })
          } else {
            log.info("RESULT CONSTRUCTED")
            gatsbyProcess.send({
              type: `JOB_COMPLETED`,
              payload: {
                id: msg.payload.id,
                result: msg.payload.args.operations,
              },
            })
          }
        } catch (error) {
          log.error(`Processing failed`, msg.payload.id, ` error:`, error)
          gatsbyProcess.send({
            type: `JOB_FAILED`,
            payload: { id: msg.payload.id, error: error.toString() },
          })
        }
        break
      }
      case MESSAGE_TYPES.LOG_ACTION:
        // msg.action.payload.text && console.log(msg.action.payload.text)
        break
      default:
        log.warn(`Ignoring message: `, msg)
    }
  }
}

exports.build = async function(cmd = `node_modules/.bin/gatsby build`) {
  log.setLevel(process.env.PARALLEL_RUNNER_LOG_LEVEL || `warn`)

  process.env.ENABLE_GATSBY_EXTERNAL_JOBS = true

  const processors = {}
  const processorList = await resolveProcessors()
  await Promise.all(
    processorList.map(async processorSettings => {
      const klass = require(processorSettings.path).Processor
      const pubSubImplementation = await new GoogleFunctions({
        processorSettings,
      })
      const processorQueue = new ProcessorQueue({ pubSubImplementation })

      processors[processorSettings.key] = new klass(processorQueue)
    })
  )

  const [bin, ...args] = cmd.split(` `)
  const gatsbyProcess = cp.fork(path.join(process.cwd(), bin), args)
  gatsbyProcess.on(`exit`, async code => {
    log.debug(`Gatsby existed with`, code)
    process.exit(code)
  })

  const handler = messageHandler(gatsbyProcess, processors)
  gatsbyProcess.on(`message`, handler)
}

exports.messageHandler = messageHandler
