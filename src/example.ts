import { createClient } from './index.js'

interface TaskDescription {
  name: string
  id: number
}

const quppy = await createClient<TaskDescription>(':memory:')

async function doWork({ name, id }: TaskDescription) {
  await new Promise<void>((resolve, reject) => {
    setTimeout(() => {
      if (Math.random() < 0.6) {
        console.log(`Job ${id} succeeded`)
        resolve()
      } else {
        console.log(`Job ${id} failed`)
        reject(new Error('random error'))
      }
    }, Math.random() * 2_000)
  })
}

for (let i = 1; i <= 10; i++) {
  quppy.enqueueJob({
    name: 'Hello',
    id: i,
    max_attempts: 10,
    time_limit_seconds: 1,
  })
}

quppy.startWorkers(doWork, 5)
