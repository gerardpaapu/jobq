# @donothing/jobq

A job queue backed by sqlite.

Strictly speaking this doesn't make any sense, since you're likely running you app server on the same machine that has the sqlite database ... so your workers are on that same machine, and you're limited to the number of cores you have on that machine.

So you should be backed by a service like redis that can be accessed by other machines so you can scale your workers out.

Still, I have some students who need to get out of the habit of putting long-running processes into their request-handlers, and I don't want to spin up a bunch of new infra for them.

So think of this as a Fisher Price: My First Work Queue. Maybe this was all a silly idea and I should just spin up a RabbitMQ instance or something...

... anyway, connect from your app server like this:

```ts
import { createClient } from '@donothing/jobq'

type MyJobDescription = 
  | { type: 'download', url: string, storage: string }

const client = await createClient<JobDescription>(process.env.QUEUE_DB_PATH)

client.enqueueJob({
  type: 'download',
  url: 'http://roms.com/pokemon.zip',
  storage: 'C:\\roms',
  maximum_attempts: 10,
  time_limit_seconds: 60,
})
```
... and then in your worker process:
```ts
import { createClient } from '@donothing/jobq'

type MyJobDescription = 
  | { type: 'download', url: string, storage: string }

const client = await createClient<JobDescription>(process.env.QUEUE_DB_PATH)
const MAX_WORKERS = 4

client.startWorkers(async ({ url, storage }) => {
  const res = await fetch(url)
  await fs.writeFile(storage, res.body)
}, MAX_WORKERS)
```
