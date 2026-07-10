"""Asset state store accessed from *within an asset-watcher trigger* -- IT DAG (AIP-103).

A ``BaseEventTrigger`` attached to an ``Asset`` via ``AssetWatcher`` reads and
writes that asset's state store through ``self.asset_state_store`` (auto-bound to
the watched asset, so no subscripting), does a get / set / delete round-trip, and
only then yields its success event. A broken (or missing) round-trip leaves the
watcher erroring instead of recording its writes.

This is the *watcher* shape AIP-103 supports for triggers: ``asset_state_store`` is
only injected on ``BaseEventTrigger`` subclasses wired to an asset (it is *not*
available to plain ``BaseTrigger`` subclasses used for task deferral). The watcher
trigger is created only while its asset belongs to an active (unpaused) DAG that
schedules on it -- hence the ``schedule=[ASSET]`` consumer DAG below.

NOTE: accessing the asset state store from a trigger requires apache/airflow#68900,
which first ships in Astro Runtime 3.3-1-alpha4. On older runtimes ``AssetWatcher``
still imports, so the file parses, but the watcher trigger would have no
``asset_state_store`` accessor; the test that drives this DAG pins ``compat`` to the
runtime that ships the wiring.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

try:
    from airflow.sdk import DAG, Asset, AssetWatcher, task
    from airflow.triggers.base import BaseEventTrigger, TriggerEvent

    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False


# Name is asserted against the asset_state_store / asset tables by the test.
TRIGGER_ASSET_NAME = "state_store_it/trigger-asset"


if _AVAILABLE:

    class AssetStateStoreTrigger(BaseEventTrigger):
        """Reads/writes the watched asset's state store from inside the triggerer."""

        def serialize(self) -> tuple[str, dict[str, Any]]:
            return ("dags.asset_state_store_trigger_dag.AssetStateStoreTrigger", {})

        async def run(self) -> AsyncIterator[TriggerEvent]:
            # ``asset_state_store`` is auto-bound to the single watched asset, so
            # the accessor methods are called directly (no subscripting).
            store = self.asset_state_store

            # Counter persists on the asset across watcher restarts / DAG runs.
            count = store.get("trigger_run_count", default=0) + 1
            store.set("trigger_run_count", count)

            # set / get / delete round-trip, each gating the success event.
            store.set("scratch", {"n": 1})
            assert store.get("scratch") == {"n": 1}
            store.delete("scratch")
            assert store.get("scratch") is None

            self.log.info(
                "asset state store round-trip ok; trigger_run_count=%s", count
            )

            # The round-trip above (which is what the test asserts on) already ran.
            # A watcher only emits a TriggerEvent on a real detected event; idle far
            # past the test window before the lone ``yield`` so run() stays a valid
            # async generator while never signalling completion during the test.
            await asyncio.sleep(86_400)
            yield TriggerEvent({"status": "success", "count": count})

    ASSET = Asset(
        name=TRIGGER_ASSET_NAME,
        uri="s3://astro-agent-it/state-store-trigger",
        watchers=[
            AssetWatcher(
                name="state_store_trigger_watcher", trigger=AssetStateStoreTrigger()
            )
        ],
    )

    # Scheduling on the asset is what makes it "active" so the watcher trigger is created;
    # the task body is irrelevant to the test.
    #
    # Paused by default so this always-running watcher doesn't hold a triggerer slot in every
    # environment that mounts examples/. The asset-state-store IT unpauses it via the API and
    # the watcher trigger is created on the next persist: watcher DAGs are never cached (see
    # AINF-2199), so the agent re-sends this DAG every cycle while it is unpaused.
    with DAG(
        dag_id="asset_state_store_trigger_it",
        schedule=[ASSET],
        catchup=False,
        is_paused_upon_creation=True,
    ):

        @task
        def noop() -> None:
            return None

        noop()
