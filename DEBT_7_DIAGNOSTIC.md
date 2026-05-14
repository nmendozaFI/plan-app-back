# Debt 7 Diagnostic

Read-only diagnostic snapshot of the pytest cascade-fail under `-v`. No code, fixtures, or config were modified.

Date captured: 2026-04-29
Captured from: `plani-app-back/plani-app-back/.venv` on Windows 11.

---

## 1. Versions

```
$ ./.venv/Scripts/python.exe --version
Python 3.14.0
```

Relevant rows from `pip list` (full output saved to `debt7_piplist.txt`):

```
anyio             4.13.0
asyncpg           0.31.0
fastapi           0.135.3
greenlet          3.3.2
httpx             0.28.1
pytest            9.0.3
pytest-asyncio    1.3.0
SQLAlchemy        2.0.49
```

Notes:
- `pytest-anyio`, `pytest-randomly`, and any `aiosqlite`/`alembic` are **not installed**.
- Python 3.14 on Windows uses `ProactorEventLoop` by default — note the `proactor_events.py` frames in the tracebacks below.

---

## 2. pytest-asyncio configuration

There is **no `pyproject.toml`**, **no `setup.cfg`**, and **no root-level `conftest.py`**. Pytest config lives entirely in `pytest.ini`, plus a single `tests/conftest.py`.

### `pytest.ini` (full)

```ini
[pytest]
# Planificador de Talleres - Test Configuration

# Enable async mode for pytest-asyncio
asyncio_mode = auto

# Test discovery paths
testpaths = tests

# Verbose output
addopts = -v --tb=short

# Ignore deprecation warnings from dependencies
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning

# Markers
markers =
    asyncio: mark test as async
    slow: mark test as slow running
    integration: mark test as integration test
```

Highlights:
- `asyncio_mode = auto` — every async function is treated as a pytest-asyncio test.
- `asyncio_default_fixture_loop_scope` is **not set**.  Under pytest-asyncio 1.3.0, the default fixture loop scope is `function`, so a brand-new event loop is created for every test and every fixture.
- No custom `event_loop` fixture exists anywhere.
- No `pytest_plugins` declaration anywhere.
- `addopts = -v --tb=short` — `-v` is **always on** by default; "running without `-v`" requires `-o "addopts="` or similar overrides. (This matters for the workaround story: the cascade is not actually triggered by `-v`; it's triggered by anything that runs more than one test that touches the DB. See observations.)

### `tests/conftest.py` (full, only fixtures shown)

```python
@pytest_asyncio.fixture
async def client():
    """Async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

@pytest_asyncio.fixture
async def db_session():
    """Direct database session for test setup/cleanup."""
    async with AsyncSessionLocal() as session:
        yield session

@pytest_asyncio.fixture(autouse=True)
async def cleanup(db_session):
    """Clean up test data after each test."""
    yield
    await _cleanup_test_data(db_session)
```

All three are `pytest_asyncio.fixture` with **default scope = function** (no `scope=` argument anywhere).

---

## 3. asyncpg pool / engine fixture

There is **no test-side engine fixture**. Tests reuse the production engine declared at module scope in `app/db.py`, imported once per worker:

### `app/db.py` (full)

```python
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os, ssl
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
print("DB URL:", os.getenv("DATABASE_URL"))

ssl_context = ssl.create_default_context()

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,                       # <-- key
    connect_args={"ssl": ssl_context}
)

AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
```

Important:
- `engine` and `AsyncSessionLocal` are **module-level singletons**, created at import time. They survive across tests.
- `pool_pre_ping=True` is enabled — every checkout from the pool does a `BEGIN/ROLLBACK` ping against the cached asyncpg connection.
- The pool default class for `create_async_engine` (with `asyncpg`) is `AsyncAdaptedQueuePool` — so connections are **kept alive between tests**, bound to whatever event loop opened them.
- SSL is enabled (Neon Postgres in production / dev). The cascade traceback goes through `asyncio.sslproto`, which is loop-bound.

### Test-layer fixtures depending on the engine

`tests/conftest.py:` (all `function`-scope):

| Fixture | Depends on | Touches engine? |
|---|---|---|
| `client` (line 30) | — | Yes — ASGITransport runs FastAPI app, which uses `get_db()` → `AsyncSessionLocal` → `engine` |
| `db_session` (line 38) | — | Yes — `async with AsyncSessionLocal() as session` |
| `cleanup` (line 44, autouse) | `db_session` | Yes — runs DELETEs and `commit()` after every test |

Every test transitively depends on `engine` via at least the autouse `cleanup → db_session` chain.

---

## 4. Failing run (full suite with `-v`)

### Command

```bash
./.venv/Scripts/python.exe -m pytest -v 2>&1 | tee debt7_run.log
```

### Summary

```
================== 103 failed, 4 passed in 187.20s (0:03:07) ==================
```

The 4 passing tests:
- `tests/test_health.py::test_health_endpoint` — no DB touch.
- `tests/test_health.py::test_root_endpoint` — no DB touch.
- `tests/test_frecuencias.py::test_calcular_frecuencias_existing_trimestre` — first test in its file; pool happens to be empty at that point.
- `tests/test_settings.py::test_update_settings_invalid_trimestre` — likely fails validation (422) before DB touch.

Process exit code: 1.

### First failure — NOT an asyncio failure (separate latent bug surfaced first)

`tests/test_calendario.py::test_generar_calendario` (the very first test pytest runs):

```
app\routers\calendario.py:97: in generar_calendario
    anio = int(trimestre.split("-")[0])
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E   ValueError: invalid literal for int() with base 10: 'TEST'
```

Root cause: tests pass `trimestre = "TEST-Q1"` (constant in `tests/conftest.py:23`), but
`app/routers/calendario.py:97` does `int(trimestre.split("-")[0])` expecting a year prefix. **This is a separate latent bug and is unrelated to the event-loop cascade**, but it is the first failure shown in the FAILURES section.

It does, however, mean test #1 errors **inside the FastAPI request**, and an in-flight asyncpg connection is returned to the pool in some state — which is plausibly what poisons the pool for test #2.

### Second failure — first occurrence of the cascade

`tests/test_calendario.py::test_generar_calendario_status_optimal_or_feasible`:

```
..\..\..\..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\asyncio\base_events.py:827: in call_soon
    self._check_closed()
..\..\..\..\..\AppData\Local\Python\pythoncore-3.14-64\Lib\asyncio\base_events.py:550: in _check_closed
    raise RuntimeError('Event loop is closed')
E   RuntimeError: Event loop is closed

During handling of the above exception, another exception occurred:
tests\test_calendario.py:35: in test_generar_calendario_status_optimal_or_feasible
    await setup_test_config_trimestral(db_session, TEST_TRIMESTRE)
tests\conftest.py:170: in setup_test_config_trimestral
    result = await db.execute(...)
sqlalchemy/ext/asyncio/session.py:449: in execute
    result = await greenlet_spawn(...)
sqlalchemy/orm/session.py:1187: in _connection_for_bind
    conn = bind.connect()
sqlalchemy/engine/base.py:3317: in raw_connection
    return self.pool.connect()
sqlalchemy/pool/base.py:1309: in _checkout
    result = pool._dialect._do_ping_w_event(...)        # <-- pool_pre_ping
sqlalchemy/dialects/postgresql/asyncpg.py:1160: in do_ping
    dbapi_connection.ping()
sqlalchemy/dialects/postgresql/asyncpg.py:825: in _async_ping
    await tr.start()
asyncpg/transaction.py:146: in start
    await self._connection.execute(query)
asyncpg/connection.py:354: in execute
    result = await self._protocol.query(query, timeout)
asyncpg/protocol/protocol.pyx:956: in BaseProtocol._write
    ???
asyncio/sslproto.py:222: in write
    self._ssl_protocol._write_appdata((data,))
asyncio/sslproto.py:700: in _write_appdata
    self._fatal_error(ex, 'Fatal error on SSL protocol')
asyncio/sslproto.py:918: in _fatal_error
    self._transport._force_close(exc)
asyncio/proactor_events.py:152: in _force_close
    self._loop.call_soon(self._call_connection_lost, exc)
asyncio/base_events.py:827: in call_soon
    self._check_closed()
E   RuntimeError: Event loop is closed

------------------------------ Captured log call ------------------------------
ERROR  sqlalchemy.pool.impl.AsyncAdaptedQueuePool: Exception terminating connection
  ...
  File "asyncpg/connection.py", line 1682, in _cancel_current_command
    self._cancellations.add(self._loop.create_task(self._cancel(waiter)))
  File "asyncio/base_events.py", line 466, in create_task
    self._check_closed()
  File "asyncio/base_events.py", line 550, in _check_closed
    raise RuntimeError('Event loop is closed')
RuntimeError: Event loop is closed
```

### Third failure — identical shape

`tests/test_calendario.py::test_calendario_no_festivo_slots`: byte-for-byte same traceback as the second (line numbers differ only because object addresses are different — same call chain: `setup_test_config_trimestral → db_session.execute → pool ping → asyncpg → sslproto → loop closed`).

All 102 cascade failures share this exact shape — the underlying asyncpg `Connection` object is bound to a previous test's now-closed event loop, `pool_pre_ping` tries to ping it, the SSL transport tries to schedule `_call_connection_lost` on the dead loop and raises.

### Last 50 lines of `debt7_run.log`

(All cascade failures; included here for completeness — see file `debt7_run.log` in repo root.)

```
... (84 FAILED lines, all "RuntimeError: Event loop is closed") ...
================== 103 failed, 4 passed in 187.20s (0:03:07) ==================
<sys>:0: RuntimeWarning: coroutine 'Connection._cancel' was never awaited
```

The trailing `RuntimeWarning: coroutine 'Connection._cancel' was never awaited` is the smoking gun: asyncpg's `_cancel_current_command` schedules a coroutine on `self._loop`, that loop is dead, the coroutine is never awaited, GC eventually warns.

---

## 5. Determinism check

`pytest-randomly` is **not installed**, so `-p no:randomly` is moot. Re-ran the suite with `--tb=line`:

```
=========== 103 failed, 4 passed, 36 warnings in 107.07s (0:01:47) ============
```

Identical: same 103 fail, same 4 pass, same order. **Deterministic** — not order-dependent or flaky.

### Single-file run

```bash
./.venv/Scripts/python.exe -m pytest -v --tb=line tests/test_talleres.py
```

```
tests/test_talleres.py::test_listar_talleres                FAILED
tests/test_talleres.py::test_talleres_have_required_fields  FAILED
tests/test_talleres.py::test_talleres_distribution          FAILED
... (5 more) ...
=================== 7 failed, 1 passed, 3 warnings in 8.32s ===================
```

**Single file with `-v` also cascades.** Only the *first* test in the file passes (fresh pool), every subsequent test fails. So the bug is **not specific to `-v`** — it triggers any time pytest runs more than one test that touches the engine in a single process. The reason "running tests one by one" works is that each invocation is a fresh Python process with a fresh import of `app.db` and an empty pool.

---

## 6. Quick sanity checks

- `asyncio.run(...)` in test code or fixtures? **No.** Only two hits, both in standalone scripts (`scripts/audit_restricciones.py:259`, `scripts/test_franja_v16.py:224`), neither of which runs under pytest.
- `new_event_loop` / `loop.close` anywhere in the repo? **No matches** outside `.venv`.
- Session/module/class/package-scoped fixtures touching async resources? **None.** No `scope="session"`, `scope="module"`, `scope="class"`, or `scope="package"` anywhere outside `.venv`. Every fixture in `tests/conftest.py` is implicit `function`-scope.

---

## 7. Initial observations

- **Root cause is structural, not flag-related.** The `engine` (and its `AsyncAdaptedQueuePool`) is a module-level singleton in `app/db.py`. Tests run one event loop per test (pytest-asyncio 1.3.0 default with no `asyncio_default_fixture_loop_scope` set). When test #1 ends, its loop closes — but the asyncpg `Connection` it created is still cached in the SQLAlchemy pool, **bound to that dead loop**. Test #2 gets the stale connection from the pool, `pool_pre_ping=True` issues a SQL ping on it, the SSL transport tries to schedule `_call_connection_lost` on the closed loop and raises `RuntimeError: Event loop is closed`. The cascade is just every subsequent test hitting this same condition.

- **`-v` is incidental.** The single-file run reproduces the same cascade — `-v` is always on via `addopts` and is not the trigger. The user's mental model ("`-v` cascade-fails") is actually "any in-process run of more than one DB-touching test cascade-fails." The workaround works because each per-test invocation is a fresh Python process.

- **`pool_pre_ping=True` (added prudently for prod) makes the failure surface immediately.** Without it, the dead connection would still be unusable but the failure would surface inside the actual query rather than in pre-ping. Same root cause either way.

- **Two layered failure modes hide each other.** The very first failure (`int('TEST')` in `app/routers/calendario.py:97`) is a separate latent bug — `TEST_TRIMESTRE = "TEST-Q1"` doesn't have a 4-digit year prefix. The router crashes during request handling, the in-flight asyncpg connection returns to the pool, and from then on the asyncio cascade obscures everything. Fixing the asyncio cascade will unmask this and probably 1–2 other latent test bugs.

- **Likely fix shapes (not implementing — observation only).**
  1. Switch tests off the production singleton: have `tests/conftest.py` build its own `engine` per test (or per session with `NullPool`) and override the FastAPI `get_db` dependency to use it. Cleanest, isolates tests from prod config.
  2. Use `poolclass=NullPool` for the test engine so no connection survives between tests; each test opens and closes its own asyncpg connection on its own loop. Slower but bulletproof on Windows + ProactorEventLoop + SSL.
  3. Bump `asyncio_default_fixture_loop_scope` to `session` and add a session-scoped event loop. This keeps the loop alive across tests so cached connections stay valid. Risk: pytest-asyncio's session-scope loop interacts subtly with autouse function-scope fixtures, and Windows ProactorEventLoop has its own quirks here.

- **Python 3.14 + Windows `ProactorEventLoop` + asyncpg over SSL** is a known fragile combo for the symptom shown (the `_force_close → call_soon → _check_closed` chain through `asyncio.sslproto` is exactly the trigger Microsoft / asyncpg issues describe). Worth keeping in mind: dropping SSL in tests (e.g., a non-SSL test database) would not fix the underlying singleton-pool issue but would make the secondary "can't even close the dead connection" warning go away.
