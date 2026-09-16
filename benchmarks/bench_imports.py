from gc import collect
from pathlib import Path
from types import ModuleType
from json import dumps, loads
from statistics import median
from time import perf_counter
from typing import get_type_hints
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from subprocess import run as run_process
from platform import platform, python_version
from sys import executable, modules, path as module_path

_CHILD = """
from sys import argv, path
from time import perf_counter
path[:0] = argv[1:3]
start = perf_counter()
import benchmark_generated
elapsed = (perf_counter() - start) * 1000
assert len(benchmark_generated._SCHEMA.tables) == int(argv[3])
assert benchmark_generated.Record0(id=1, **dict.fromkeys(
    (f'field_{index}' for index in range(12)), 'value'
)).field_0 == 'value'
print(elapsed)
"""

def measure_pair(actions, samples, verify=lambda value: None):
    timings = {name: [] for name in actions}
    for index in range(samples + 1):
        names = tuple(actions)
        if index % 2:
            names = names[::-1]

        for name in names:
            collect()
            start = perf_counter()
            value = actions[name]()
            elapsed = (perf_counter() - start) * 1000
            verify(value)
            del value
            if index:
                timings[name].append(elapsed)

    return {name: {"median_ms": median(values), "samples_ms": values} for name, values in timings.items()}

def main():
    parser = ArgumentParser(description="Paired generated-model annotation benchmarks on the current interpreter")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", type=int, default=11)
    arguments = parser.parse_args()
    if arguments.samples < 1:
        parser.error("--samples must be positive")

    source_root = arguments.source_root.resolve()
    module_path.insert(0, str(source_root))

    from sqrrl.generate import render
    from sqrrl.schema import Schema, Table, integer, text

    result = {
        "python": python_version(),
        "platform": platform(),
        "source_root": str(source_root),
        "samples": arguments.samples,
        "comparison": "Stringified versus deferred generated annotations; same interpreter and library code",
        "workloads": {},
    }
    with TemporaryDirectory(prefix="sqrrl-import-bench-") as temporary:
        root = Path(temporary)
        for count in (1, 25, 150):
            schema = Schema(
                    tuple(
                            Table(
                                    f"records_{index}",
                                    f"Record{index}",
                                    (integer("id").primary_key(), *(text(f"field_{number}") for number in range(12))),
                            )
                            for index in range(count)
                    )
            )
            deferred = render(schema)
            assert "from __future__ import annotations" not in deferred
            sources = {"stringified": "from __future__ import annotations\n" + deferred, "deferred": deferred}
            workloads = {}
            result["workloads"][f"{count}_tables"] = workloads
            workloads["compile"] = measure_pair(
                    {
                        name: lambda source=source: compile(source, "benchmark_generated.py", "exec", dont_inherit=True)
                        for name, source in sources.items()
                    },
                    arguments.samples,
            )
            codes = {
                name: compile(source, "benchmark_generated.py", "exec", dont_inherit=True)
                for name, source in sources.items()
            }

            def initialize(code):
                generated = ModuleType("benchmark_generated")
                modules[generated.__name__] = generated
                try:
                    exec(code, generated.__dict__)
                except BaseException:
                    modules.pop(generated.__name__, None)
                    raise

                return generated

            def verify(generated):
                try:
                    assert generated._SCHEMA == schema.normalize()
                    for index in {0, count - 1}:
                        model = getattr(generated, f"Record{index}")
                        hints = get_type_hints(model)
                        assert hints == {"id": int, **dict.fromkeys((f"field_{i}" for i in range(12)), str)}
                        assert model(id=1, **dict.fromkeys((f"field_{i}" for i in range(12)), "value")).id == 1
                        repository = getattr(generated, f"Record{index}Repository")
                        assert get_type_hints(repository.get)["return"] is model
                finally:
                    modules.pop(generated.__name__, None)

            workloads["initialize_precompiled"] = measure_pair(
                    {name: lambda code=code: initialize(code) for name, code in
                     codes.items()}, arguments.samples, verify
            )
            child_timings = {name: [] for name in sources}
            directories = {}
            for name, source in sources.items():
                directory = root / f"{count}_{name}"
                directory.mkdir()
                (directory / "benchmark_generated.py").write_text(source, encoding="utf-8")
                directories[name] = directory

            def cold_import(name):
                process = run_process(
                        [executable, "-B", "-c", _CHILD, str(source_root), str(directories[name]), str(count)],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=60,
                )
                child_timings[name].append(loads(process.stdout))

            workloads["fresh_process_wall"] = measure_pair(
                    {name: lambda name=name: cold_import(name) for name in sources}, arguments.samples
            )
            workloads["fresh_process_import"] = {
                name: {"median_ms": median(values[1:]), "samples_ms": values[1:]}
                for name, values in child_timings.items()
            }
            for workload, measurements in workloads.items():
                before = measurements["stringified"]["median_ms"]
                after = measurements["deferred"]["median_ms"]
                print(f"{count} tables {workload}: {before:.3f} -> {after:.3f} ms ({(after / before - 1) * 100:+.1f}%)")

    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(dumps(result, indent=2) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()
