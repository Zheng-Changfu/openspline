"""Manual real-process/HTTP lifecycle probe; no GPU, mocks, credentials or user media."""

import asyncio
import io
import json
import multiprocessing
import os
import signal
import sys
import tempfile
from pathlib import Path

import httpx
import uvicorn
from PIL import Image

from openspline_server.app import create_app
from openspline_server.config import Settings


async def wait_until(predicate, seconds=15):
    async with asyncio.timeout(seconds):
        while not predicate():
            await asyncio.sleep(0.05)


def resume(pid):
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        pass


def snapshot(app, folder, reaper):
    worker = app.state.pool.workers[0]
    return {
        "sessions": len(app.state.sessions),
        "owner_retained": worker.owner is not None,
        "worker_ready": worker.ready,
        "session_directory_exists": folder.exists(),
        "reaper_running": not reaper.done(),
        "reaper_error": type(reaper.exception()).__name__ if reaper.done() else None,
    }


async def run(variant, mode):
    result = {
        "variant": variant,
        "mode": mode,
        "base_sha": "1e5f2b06e6c3dfc68e59ab9bfb3b30997d51caf9",
        "backend": "test",
        "real_process_ipc_http": True,
        "gpu_and_media_playback_verified": False,
    }
    with tempfile.TemporaryDirectory(prefix="openspline-lifecycle-") as runtime:
        settings = Settings(
            backend="test",
            runtime_dir=runtime,
            public_url="http://127.0.0.1:10580",
            model_dir="/tmp/unused-models",
            audio_model_dir="/tmp/unused-models",
            idle_timeout=1 if mode == "expiry" else 3600,
        )
        app = create_app(settings)
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=10580, log_level="warning"))
        serving = asyncio.create_task(server.serve())
        stopped_pid = None
        try:
            await wait_until(lambda: server.started or serving.done())
            assert server.started, "HTTP server failed to start"
            reaper = next(
                task for task in asyncio.all_tasks()
                if task.get_coro().__qualname__.endswith("create_app.<locals>.reaper")
            )
            portrait = io.BytesIO()
            Image.new("RGB", (64, 64), (24, 72, 120)).save(portrait, "PNG")
            async with httpx.AsyncClient(base_url=settings.public_url, timeout=10) as client:
                response = await client.post("/v1/sessions", files={"portrait": ("synthetic.png", portrait.getvalue(), "image/png")})
                assert response.status_code == 201, response.status_code
                descriptor = response.json()
                session_id = descriptor["id"]
                folder = Path(runtime) / "sessions" / session_id
                worker = app.state.pool.workers[0]
                stopped_pid = worker.process.pid
                os.kill(stopped_pid, signal.SIGSTOP)
                await wait_until(lambda: Path(f"/proc/{stopped_pid}/stat").read_text().split(") ", 1)[1].startswith("T"))
                worker.conn.close()
                result["fault"] = "SIGSTOP real worker, close real parent Connection, then reset sends on closed IPC"
                if mode == "delete":
                    response = await client.delete(f"/v1/sessions/{session_id}", headers={"Authorization": f"Bearer {descriptor['publisher_token']}"})
                    result["delete_status"] = response.status_code
                    assert response.status_code == 500, response.status_code
                else:
                    await wait_until(lambda: reaper.done() or (not app.state.sessions and worker.owner is None))
                result["after_failed_release"] = snapshot(app, folder, reaper)
                resume(stopped_pid)
                stopped_pid = None
                if variant == "proposed":
                    await wait_until(lambda: worker.ready and worker.owner is None)
                else:
                    await asyncio.sleep(6)
                result["after_recovery_cycle"] = snapshot(app, folder, reaper)
                second = await client.post("/v1/sessions", files={"portrait": ("synthetic.png", portrait.getvalue(), "image/png")})
                result["second_create_status"] = second.status_code
                if second.status_code == 201:
                    second_descriptor = second.json()
                    second_id = second_descriptor["id"]
                    if mode == "expiry":
                        await wait_until(lambda: second_id not in app.state.sessions)
                        status = await client.get(f"/v1/sessions/{second_id}", headers={"Authorization": f"Bearer {second_descriptor['publisher_token']}"})
                        result["second_expired_status"] = status.status_code
                    else:
                        closed = await client.delete(f"/v1/sessions/{second_id}", headers={"Authorization": f"Bearer {second_descriptor['publisher_token']}"})
                        result["second_delete_status"] = closed.status_code
                result["before_harness_cleanup"] = snapshot(app, folder, reaper)
        finally:
            if stopped_pid is not None:
                resume(stopped_pid)
            server.should_exit = True
            try:
                await asyncio.wait_for(serving, 20)
            finally:
                # The baseline reaper exception may abort lifespan before pool.close.
                await app.state.pool.close()
        result["remaining_worker_processes"] = len(multiprocessing.active_children())
        assert result["remaining_worker_processes"] == 0
        before = result["after_failed_release"]
        if variant == "proposed":
            assert not before["owner_retained"] and not before["session_directory_exists"]
            assert before["reaper_running"] and result["second_create_status"] == 201
            assert result["before_harness_cleanup"]["sessions"] == 0
            if mode == "expiry":
                assert result["second_expired_status"] == 404
        else:
            assert before["owner_retained"] and before["session_directory_exists"]
            assert result["second_create_status"] == 429
            if mode == "expiry":
                assert before["reaper_error"] == "OSError"
        result["expected_comparison_passed"] = True
    result["scratch_directory_removed"] = not Path(runtime).exists()
    return result


if __name__ == "__main__":
    variant, mode = sys.argv[1:3]
    outcome = asyncio.run(run(variant, mode))
    print(json.dumps(outcome, indent=2), flush=True)
    Path(f"/results/{variant}-{mode}.json").write_text(json.dumps(outcome, indent=2) + "\n")
