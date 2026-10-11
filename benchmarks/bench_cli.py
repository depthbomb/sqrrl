from os import environ
from pathlib import Path
from sys import executable
from json import dumps, loads
from statistics import median
from time import perf_counter
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from subprocess import run as run_process
from platform import platform, python_version

def invoke(source_root, directory, arguments):
    environment = dict(environ, PYTHONPATH=str(source_root), NO_COLOR='1')
    return run_process(
        [executable, '-m', 'sqrrl', *arguments],
        cwd=directory,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )

def main():
    parser = ArgumentParser(description='Paired CLI startup benchmarks in fresh Python processes')
    parser.add_argument('--baseline-root', type=Path, required=True)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--samples', type=int, default=21)
    parser.add_argument('--output', type=Path)
    arguments = parser.parse_args()
    if arguments.samples < 1:
        parser.error('--samples must be positive')

    roots = {'baseline': arguments.baseline_root.resolve(), 'candidate': arguments.source_root.resolve()}
    for root in roots.values():
        if not (root / 'sqrrl' / 'cli.py').is_file():
            parser.error(f'Missing sqrrl/cli.py in {root}')

    result = {
        'python': python_version(),
        'platform': platform(),
        'roots': {name: str(root) for name, root in roots.items()},
        'samples': arguments.samples,
        'comparison': 'Fresh-process CLI wall time; alternating order, warm bytecode, same interpreter',
        'workloads': {},
    }
    commands = {'help': ('--help',), 'migrate_help': ('migrate', '--help'), 'init': ('init',),
                'generate_check': ('generate', '--check')}
    with TemporaryDirectory(prefix='sqrrl-cli-bench-') as temporary:
        project = Path(temporary)
        invoke(roots['baseline'], project, ('init',))
        invoke(roots['baseline'], project, ('generate',))
        for workload, command in commands.items():
            timings = {name: [] for name in roots}
            for index in range(arguments.samples + 1):
                names = tuple(roots)
                if index % 2:
                    names = names[::-1]

                outputs = []
                for name in names:
                    directory = project
                    if workload == 'init':
                        directory = project / f'{name}_{index}'
                        directory.mkdir()

                    start = perf_counter()
                    process = invoke(roots[name], directory, command)
                    elapsed = (perf_counter() - start) * 1000
                    outputs.append((process.stdout, process.stderr))
                    if workload == 'init':
                        assert loads((directory / 'sqrrl.json').read_text(encoding='utf-8'))['schema'] == 'schema:schema'
                        assert (directory / 'schema.py').is_file()
                    if index:
                        timings[name].append(elapsed)

                assert outputs[0] == outputs[1], outputs

            measurements = {name: {'median_ms': median(values), 'samples_ms': values}
                            for name, values in timings.items()}
            result['workloads'][workload] = measurements
            before = measurements['baseline']['median_ms']
            after = measurements['candidate']['median_ms']
            print(f'{workload}: {before:.3f} -> {after:.3f} ms ({(after / before - 1) * 100:+.1f}%)')

    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(dumps(result, indent=2) + '\n', encoding='utf-8')

if __name__ == '__main__':
    main()
