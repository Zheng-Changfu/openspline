OpenSpline lifecycle IPC failure acceptance

Base: openspline/openspline 1e5f2b06e6c3dfc68e59ab9bfb3b30997d51caf9.
Run the same probe against the untouched checkout and the proposed lifecycle patch.
The provided manifest records the actual dependency versions and patch checksum.

Scope: the official backend=test replaces GPU inference only. WorkerPool uses real
spawned processes and multiprocessing pipes. Uvicorn serves the real FastAPI app
over container loopback; its real reaper expires sessions. No methods are mocked.
The portrait is a generated solid-color PNG. No external service credentials or
user media are used. Session publisher tokens are generated locally, kept in
memory, and never included in artifacts. No LiveKit rooms are used.

The fault deliberately pauses the real worker using SIGSTOP, then closes its
parent multiprocessing Connection. Pausing makes is_alive() true so reset sends
on the closed connection and raises OSError. This tests a resource-failure path;
it does not establish how frequently that path occurs in production. Ordinary
GPU/model/lock startup failure is a different path and is not evidence for this
owner leak: existing Worker.start records such initialization errors.

Invocation in a Linux environment with the server dependencies installed:
  PYTHONPATH=<checkout>/packages/server/src:<checkout>/packages/python/src \
    python probe.py baseline delete
  python probe.py baseline expiry
Use proposed in place of baseline when the proposed patch is applied.
The same PYTHONPATH must be used for all runs. Provide a writable /results
directory. Each invocation uses a fresh process and loopback port 10580.
Our runs used four separate --rm containers with --network none, no GPUs,
two CPUs and a 2 GiB memory limit. Source files were mounted read-only.

Results:
- DELETE: both return HTTP 500. Baseline retains owner and session directory;
  next session returns 429. Proposed relinquishes owner and removes the directory;
  reaper restores worker availability, next session returns 201 and deletes 204.
- Expiry: baseline reaper exits with OSError, retaining owner and directory;
  next session returns 429. Proposed reaper continues, restores availability,
  accepts another session, and expires it (GET returns 404).

All four probes matched the expected comparison and ended with no child worker
processes. A harness finally block closes the pool even if baseline lifespan
fails, and TemporaryDirectory removes scratch data. This harness cleanup is
separate from the before_harness_cleanup observations; it is not evidence that
baseline application cleanup succeeded. The four containers were removed.

Not validated: GPU inference, native media or playback, injected media-resource
close failures, attach/close concurrency, or repeated cancellation. This is a
lifecycle fault acceptance, not a full call acceptance.
