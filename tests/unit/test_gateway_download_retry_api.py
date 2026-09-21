import asyncio

import pytest


def test_gateway_and_workers_share_the_default_output_root(monkeypatch):
    from gateway.config import GatewaySettings
    from runtime.paths import OUTPUTS_ROOT
    monkeypatch.delenv('FLOW_GATEWAY_OUTPUT_DIR', raising=False)
    assert GatewaySettings.from_env().output_root == OUTPUTS_ROOT


@pytest.mark.asyncio
@pytest.mark.parametrize('outside', [False, True])
async def test_video_delivery_checks_configured_root_and_returns_mp4(monkeypatch, tmp_path, outside):
    from fastapi import HTTPException
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from gateway import main
    root = tmp_path/'outputs'
    root.mkdir()
    video = (tmp_path if outside else root)/'result.mp4'
    video.write_bytes(b'\x00\x00\x00\x18ftypmp42'+b'x'*2048)
    monkeypatch.setattr(main.settings, 'output_root', root)
    monkeypatch.setattr(main, 'scheduler', SimpleNamespace(get_task=AsyncMock(return_value={
        'status': 'completed', 'video_path': str(video)})))
    if outside:
        with pytest.raises(HTTPException) as failure:
            await main.v1_client_download_task('test')
        assert failure.value.status_code == 409
        assert 'outside allowed output' in failure.value.detail
    else:
        result = await main.v1_client_download_task('test')
        assert result.media_type == 'video/mp4'
        assert result.path == str(video) or result.path == video


@pytest.mark.asyncio
async def test_recovering_download_still_occupies_a_concurrency_slot(tmp_path):
    from gateway import crud
    from gateway.config import GatewaySettings
    from tests.unit.test_gateway_dry_run import FakeRealWorkerClient, make_scheduler
    worker = FakeRealWorkerClient({'FLOW-001': {'credits': 100}})
    scheduler = make_scheduler(GatewaySettings(db_path=tmp_path/'slots.db', dry_run=False), worker)
    await scheduler.start()
    scheduler._runner_task.cancel()
    await asyncio.gather(scheduler._runner_task, return_exceptions=True)
    try:
        task = await crud.create_task(scheduler.db, {'idempotency_key': 'slot', 'image_path': 'image.png',
            'prompt': 'original', 'duration': 10, 'aspect_ratio': '9:16'})
        await crud.update_task_status(scheduler.db, task['task_id'], 'waiting_recovery', worker_job_id='existing')
        assert (await scheduler.pool_status())['active_count'] == 1
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong_result', [False, True])
async def test_download_retry_completes_same_job_without_generating_or_charging(tmp_path, wrong_result):
    from gateway import crud
    from gateway.config import GatewaySettings
    from tests.unit.test_gateway_dry_run import FakeRealWorkerClient, make_scheduler

    worker = FakeRealWorkerClient({'FLOW-004': {'credits': 100}})
    scheduler = make_scheduler(GatewaySettings(db_path=tmp_path/'gateway.db', dry_run=False), worker)
    await scheduler.start()
    scheduler._runner_task.cancel()
    await asyncio.gather(scheduler._runner_task, return_exceptions=True)
    try:
        task = await crud.create_task(scheduler.db, {'idempotency_key': 'recover', 'image_path': 'image.png',
            'prompt': 'original', 'duration': 10, 'aspect_ratio': '9:16'})
        await crud.update_task_status(scheduler.db, task['task_id'], 'download_failed',
            worker_job_id='saved-job', assigned_account_id='FLOW-004', project_id='saved-project',
            output_media_id='saved-media', generation_attempts=1, attempt_count=1)
        video = tmp_path/'existing.mp4'
        video.write_bytes(b'\x00\x00\x00\x18ftypmp42'+b'x'*2048)
        worker.jobs['saved-job'] = {'status': 'failed', 'video_path': str(video),
            'project_id': 'other-project' if wrong_result else 'saved-project', 'output_media_id': 'saved-media'}
        result = await scheduler.retry_download(task['task_id'])
        assert result['status'] == ('download_failed' if wrong_result else 'completed')
        if wrong_result:
            assert result['error_code'] == 'remote_id_conflict'
        assert result['worker_job_id'] == 'saved-job'
        assert result['generation_attempts'] == result['attempt_count'] == 1
        assert worker.submits == []
        assert worker.retry_downloads == [('FLOW-004', 'saved-job')]
        account = await crud.get_account(scheduler.db, 'FLOW-004')
        assert account['current_task_id'] is None
        assert account['consumed_credits'] == 0
    finally:
        await scheduler.stop()


@pytest.mark.asyncio
async def test_manual_recovery_keeps_active_download_in_progress(tmp_path):
    from unittest.mock import AsyncMock
    from gateway import crud
    from gateway.config import GatewaySettings
    from tests.unit.test_gateway_dry_run import FakeRealWorkerClient, make_scheduler
    worker = FakeRealWorkerClient({'FLOW-004': {'credits': 100}})
    scheduler = make_scheduler(GatewaySettings(db_path=tmp_path/'gateway.db', dry_run=False), worker)
    await scheduler.start()
    scheduler._runner_task.cancel()
    await asyncio.gather(scheduler._runner_task, return_exceptions=True)
    scheduler._run_real_task = AsyncMock()
    try:
        task = await crud.create_task(scheduler.db, {'idempotency_key': 'running-recover', 'image_path': 'image.png',
            'prompt': 'original', 'duration': 10, 'aspect_ratio': '9:16'})
        await crud.update_task_status(scheduler.db, task['task_id'], 'download_failed',
            worker_job_id='saved-job', assigned_account_id='FLOW-004', project_id='saved-project',
            output_media_id='saved-media', generation_attempts=1, attempt_count=1)
        worker.retry_omni_video_download = AsyncMock(return_value={'status': 'downloading',
            'download_in_progress': True, 'project_id': 'saved-project', 'output_media_id': 'saved-media'})
        result = await scheduler.retry_download(task['task_id'])
        assert result['status'] == 'downloading'
        await asyncio.sleep(0)
        scheduler._run_real_task.assert_awaited_once_with(task['task_id'], 'FLOW-004')
        assert worker.submits == []
        assert result['generation_attempts'] == 1
    finally:
        await scheduler.stop()
