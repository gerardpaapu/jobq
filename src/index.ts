import sqlite3 from 'sqlite3'

const StatusType = {
  PENDING: 1,
  IN_PROGRESS: 2,
  COMPLETE: 3,
  FAILED: 4,
} as const

const BACKOFF_K = 50

type Status = 1 | 2 | 3 | 4

interface JobParams {
  description: string
  time_limit_seconds: number
  max_attempts: number
}

interface Job {
  id: number
  status: Status
  description: string
  max_attempts: number
  attempts_remaining: number
  time_limit_seconds: number
  deadline: number
}

interface RunResult {
  changes: number | undefined
  lastID: number | undefined
}

async function getNextJob(db: sqlite3.Database) {
  return new Promise<Job | undefined>((resolve, reject) => {
    db.get(
      `UPDATE jobs
      SET
        status = 2,
        deadline = unixepoch('now', printf('+%d second', time_limit_seconds)),
        attempts_remaining = attempts_remaining - 1
      WHERE id = (
        SELECT id
        FROM jobs
        WHERE (status = ${StatusType.PENDING} AND attempts_remaining > 0)
        -- IN_PROGRESS jobs that are past their deadline may be 
        -- picked up by another worker
        OR (status = ${StatusType.IN_PROGRESS} 
            AND deadline < unixepoch('now')
            AND attempts_remaining > 0)
        -- We want to pick up jobs that are due soon I think?
        -- Maybe this should be randomised
        ORDER BY deadline DESC
        LIMIT 1
      )
      RETURNING *`,
      (err, value) => {
        if (err != undefined) {
          reject(err)
        } else {
          resolve(value as Job | undefined)
        }
      }
    )
  })
}

async function failJob(db: sqlite3.Database, job: Job, reason: string) {
  return new Promise((resolve, reject) => {
    if (job.attempts_remaining <= 0) {
      db.run(
        `UPDATE jobs
         SET status = ${StatusType.FAILED}, error = ?
         WHERE id = ?
         -- if this is the same number we have, then
         -- this is still our attempt, otherwise we ran out of
         -- time and someone else has picked this up
         AND attempts_remaining = ?`,
        [reason, job.id, job.attempts_remaining],
        function (this: RunResult, err: Error) {
          const { changes, lastID } = this
          if (err != undefined) {
            reject(err)
          } else {
            resolve(changes === 1)
          }
        }
      )
    } else {
      db.run(
        `UPDATE jobs
         SET 
            status = ${StatusType.PENDING},
            deadline = unixepoch('now', printf('+%d second', time_limit_seconds))
         WHERE id = ?`,
        [job.id],
        function (this: RunResult, err: Error) {
          if (err != undefined) {
            reject(err)
          } else {
            resolve(this.changes === 1)
          }
        }
      )
    }
  })
}

async function completeJob(db: sqlite3.Database, job: Job): Promise<boolean> {
  const { id, attempts_remaining } = job
  return new Promise((resolve, reject) => {
    db.run(
      `UPDATE jobs
         SET status = ${StatusType.COMPLETE}
       WHERE id = ?
       -- we don't need to check the deadline here
       -- we just check the attempts_remaining to see if someone
       -- else has picked it up
       AND attempts_remaining = ?`,
      [id, attempts_remaining],
      function (this: RunResult, err: Error) {
        if (err != undefined) {
          reject(err)
        } else {
          resolve(this.changes === 1)
        }
      }
    )
  })
}

async function enqueueJob(db: sqlite3.Database, params: JobParams) {
  return new Promise((resolve, reject) => {
    db.run(
      `INSERT INTO jobs(
          description, 
          max_attempts,
          attempts_remaining,
          time_limit_seconds,
          deadline,
          status
        ) VALUES (?, ?, ?, ?, unixepoch('now', ?), ${StatusType.PENDING})
      `,
      [
        params.description,
        params.max_attempts,
        params.max_attempts,
        params.time_limit_seconds,
        `+${params.time_limit_seconds} second`,
      ],
      function (this: RunResult, err: Error) {
        if (err != undefined) {
          reject(err)
        } else {
          resolve(this.lastID)
        }
      }
    )
  })
}

async function initialiseDB(db: sqlite3.Database): Promise<void> {
  return new Promise((resolve, reject) => {
    db.exec(
      `PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;
      PRAGMA busy_timeout = 5000;

      CREATE TABLE IF NOT EXISTS status
        ( id INTEGER PRIMARY KEY,
          name TEXT
        ) STRICT;
      
      INSERT INTO status (id, name) VALUES
          (1, 'PENDING'),
          (2, 'IN_PROGRESS'),
          (3, 'COMPLETE'),
          (4, 'FAILED')
          ON CONFLICT DO NOTHING;

      CREATE TABLE IF NOT EXISTS jobs
        ( id INTEGER PRIMARY KEY
        , description TEXT -- a JSON object
        , status INTEGER REFERENCES status(id)
        , max_attempts INTEGER
        , attempts_remaining INTEGER
        , time_limit_seconds INTEGER
        , deadline REAL -- Date
        , error TEXT
        ) STRICT;`,
      (err) => {
        if (err != undefined) {
          reject(err)
        } else {
          resolve()
        }
      }
    )
  })
}

async function getJobs(db: sqlite3.Database) {
  return new Promise<Job[]>((resolve, reject) => {
    db.all(`SELECT * FROM jobs`, (err: Error, data: Array<unknown>) => {
      if (err != undefined) {
        reject(err)
      } else {
        resolve(data as Job[])
      }
    })
  })
}

function isBusyError(e: unknown) {
  return e instanceof Error && 'errno' in e && e.errno === 5
}

async function startWorker(
  db: sqlite3.Database,
  worker: (task: string) => Promise<undefined | JobParams[]>
) {
  let busy_errors = 0
  for (;;) {
    let job: Job | undefined
    let err: unknown
    try {
      job = await getNextJob(db)
    } catch (e) {
      err = e
    }

    if (isBusyError(err)) {
      busy_errors++
      const range = BACKOFF_K * Math.pow(Math.E, busy_errors)
      const time = Math.floor(Math.random() * range)
      await sleep(time)
    } else {
      busy_errors = 0
    }

    if (!job) {
      continue
    }

    let complete
    let workError
    let newJobs
    try {
      newJobs = await worker(job.description)
      complete = true
    } catch (e) {
      workError = e
    }

    if (newJobs) {
      for (const job of newJobs) {
        enqueueJob(db, job).catch((e) => {
          console.error(`Failed to enqueue job`, e, job)
        })
      }
    }

    try {
      if (complete) {
        await completeJob(db, job)
      } else {
        await failJob(db, job, String(workError))
      }
    } catch (err) {
      console.error('Failed to finalise job', err, job)
    }
  }
}

async function sleep(n: number) {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, n)
  })
}

export async function createClient<T>(path: string) {
  type JobParamsT = T & Omit<JobParams, 'description'>
  const db = new sqlite3.Database(path)
  await initialiseDB(db)

  return {
    enqueueJob(params: JobParamsT) {
      const { time_limit_seconds, max_attempts, ...rest } = params
      const description = JSON.stringify(rest)
      enqueueJob(db, { time_limit_seconds, max_attempts, description })
    },

    getJobs(): Promise<Job[]> {
      return getJobs(db)
    },

    startWorkers(
      worker: (params: T) => Promise<void | undefined | JobParamsT[]>,
      n: number
    ) {
      for (let i = 0; i < n; i++) {
        startWorker(db, async (json: string) => {
          const t = await worker(JSON.parse(json))
          if (!t) {
            return undefined
          }

          return t.map((jt) => {
            const { max_attempts, time_limit_seconds, ...rest } = jt
            const description = JSON.stringify(jt)
            return { max_attempts, time_limit_seconds, description }
          })
        })
      }
    },
  }
}
