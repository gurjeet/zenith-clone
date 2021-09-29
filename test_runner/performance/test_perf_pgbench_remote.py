import dataclasses
import os
import subprocess
from typing import List
from fixtures.benchmark_fixture import PgBenchRunResult, ZenithBenchmarker
import pytest
from datetime import datetime
import calendar
import timeit
import os

pytest_plugins = ("fixtures.benchmark_fixture",)


def utc_now_timestamp():
    return calendar.timegm(datetime.utcnow().utctimetuple())


@dataclasses.dataclass
class PgBenchRunner:
    connstr: str
    scale: int
    transactions: int
    pgbench_bin_path: str = "pgbench"

    def invoke(self, args: List[str]):
        return subprocess.run([self.pgbench_bin_path, *args], check=True, capture_output=True)

    def init(self, vacuum: bool = True):
        args = []
        if not vacuum:
            args.append("--no-vacuum")
        args.extend([f"--scale={self.scale}", "--initialize", self.connstr])
        self.invoke(args)

    def run(self, jobs: int = 1, clients: int = 1):
        return self.invoke(
            [
                f"--transactions={self.transactions}",
                f"--jobs={jobs}",
                f"--client={clients}",
                "--progress=2",  # print progress every two seconds
                self.connstr,
            ]
        )


@pytest.fixture
def connstr():
    res = os.getenv("BENCHMARK_CONNSTR")
    if res is None:
        raise ValueError("no connstr provided, use BENCHMARK_CONNSTR environment variable")
    return res


# TODO wait for connstr availability


@pytest.mark.parametrize("scale,transactions", [(1, 1000), (2, 2000)])
@pytest.mark.remote
def test_pg_bench_remote(
    zenbenchmark: ZenithBenchmarker, connstr: str, scale: int, transactions: int
):
    pg_bin = os.getenv("PG_BIN")
    if pg_bin is not None:
        pgbench_bin_path = os.path.join(pg_bin, 'pgbench')
    else:
        pgbench_bin_path = 'pgbench'

    runner = PgBenchRunner(
        connstr=connstr,
        scale=scale,
        transactions=transactions,
        pgbench_bin_path=pgbench_bin_path,
    )
    # calculate timestamps and durations separately
    # timestamp is intended to be used for linking to grafana and logs
    # duration is actually a metric and uses float instead of int for timestamp
    init_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    runner.init()
    init_duration = timeit.default_timer() - t0
    init_end_timestamp = utc_now_timestamp()

    run_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    out = runner.run()
    run_duration = timeit.default_timer() - t0
    run_end_timestamp = utc_now_timestamp()

    res = PgBenchRunResult.parse_from_output(
        out=out,
        init_duration=init_duration,
        init_start_timestamp=init_start_timestamp,
        init_end_timestamp=init_end_timestamp,
        run_duration=run_duration,
        run_start_timestamp=run_start_timestamp,
        run_end_timestamp=run_end_timestamp,
    )

    zenbenchmark.record_pg_bench_result(res)