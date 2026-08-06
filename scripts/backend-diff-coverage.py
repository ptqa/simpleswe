#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


DEFAULT_THRESHOLD = 80.0
DEFAULT_PROFILE = Path("tmp/coverage/backend-diff.out")
BACKEND_COVERPKG = "./cmd/...,./internal/..."
BACKEND_TEST_PACKAGES = ["./cmd/...", "./internal/..."]

HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
COVER_PROFILE_RE = re.compile(r"^(.+):(\d+)\.\d+,(\d+)\.\d+\s+\d+\s+(\d+)$")


@dataclass(frozen=True)
class CoverageBlock:
    start_line: int
    end_line: int
    count: int


class CoverageError(Exception):
    pass


def run(args: list[str], *, cwd: Path, capture: bool = False) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def git_root() -> Path:
    result = run(["git", "rev-parse", "--show-toplevel"], cwd=Path.cwd(), capture=True)
    if result.returncode != 0:
        raise CoverageError("failed to determine git root")
    return Path(result.stdout.strip())


def module_path(root: Path) -> str:
    go_mod = root / "go.mod"
    for line in go_mod.read_text(encoding="utf-8").splitlines():
        fields = line.strip().split()
        if len(fields) == 2 and fields[0] == "module":
            return fields[1]
    raise CoverageError("failed to determine Go module path from go.mod")


def diff_args(compare_ref: str | None) -> list[str]:
    args = ["git", "diff"]
    if compare_ref:
        args.append(compare_ref)
    else:
        args.append("--cached")
    return args + ["--unified=0", "--diff-filter=ACMR", "--no-ext-diff", "--", "cmd", "internal"]


def is_backend_production_go_path(path: str) -> bool:
    return (
        (path.startswith("cmd/") or path.startswith("internal/"))
        and path.endswith(".go")
        and not path.endswith("_test.go")
    )


def parse_added_lines(diff_text: str) -> dict[str, set[int]]:
    added_lines: dict[str, set[int]] = {}
    current_file: str | None = None
    new_line: int | None = None

    for line in diff_text.splitlines():
        if line.startswith("+++ "):
            path = line[4:]
            current_file = None
            if path.startswith("b/"):
                candidate = path[2:]
                if is_backend_production_go_path(candidate):
                    current_file = candidate
                    added_lines.setdefault(current_file, set())
            continue

        match = HUNK_RE.match(line)
        if match:
            new_line = int(match.group(1))
            continue

        if current_file is None or new_line is None or line.startswith("\\"):
            continue

        if line.startswith("+") and not line.startswith("+++"):
            added_lines[current_file].add(new_line)
            new_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            continue
        else:
            new_line += 1

    return {path: lines for path, lines in added_lines.items() if lines}


def staged_file_lines(root: Path, path: str) -> list[str]:
    result = run(["git", "show", f":{path}"], cwd=root, capture=True)
    if result.returncode != 0:
        file_path = root / path
        if not file_path.exists():
            return []
        return file_path.read_text(encoding="utf-8", errors="replace").splitlines()
    return result.stdout.splitlines()


def worktree_file_lines(root: Path, path: str) -> list[str]:
    file_path = root / path
    if not file_path.exists():
        return []
    return file_path.read_text(encoding="utf-8", errors="replace").splitlines()


def is_meaningful_go_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if stripped.startswith(("//", "/*", "*", "*/")):
        return False
    return any(char.isalnum() or char == "_" for char in stripped)


def filter_meaningful_added_lines(
    root: Path, added_lines: dict[str, set[int]], *, staged: bool
) -> dict[str, set[int]]:
    filtered: dict[str, set[int]] = {}
    for path, lines in added_lines.items():
        file_lines = staged_file_lines(root, path) if staged else worktree_file_lines(root, path)
        meaningful = {
            line_number
            for line_number in lines
            if line_number <= len(file_lines) and is_meaningful_go_line(file_lines[line_number - 1])
        }
        if meaningful:
            filtered[path] = meaningful
    return filtered


def run_backend_tests(root: Path, profile: Path) -> None:
    profile.parent.mkdir(parents=True, exist_ok=True)
    if profile.exists():
        profile.unlink()

    command = [
        "go",
        "test",
        "-race",
        "-covermode=atomic",
        f"-coverpkg={BACKEND_COVERPKG}",
        f"-coverprofile={profile}",
        *BACKEND_TEST_PACKAGES,
    ]
    result = run(command, cwd=root)
    if result.returncode != 0:
        raise CoverageError("backend tests failed; diff coverage was not checked")
    if not profile.exists():
        raise CoverageError(f"backend tests did not create coverage profile: {profile}")


def normalize_profile_path(raw_path: str, root: Path, module: str) -> str | None:
    path = raw_path.replace("\\", "/")
    module_prefix = f"{module}/"
    if path.startswith(module_prefix):
        return path[len(module_prefix) :]
    if path.startswith("./"):
        return path[2:]
    if Path(path).is_absolute():
        try:
            return Path(path).relative_to(root).as_posix()
        except ValueError:
            return None
    return path


def parse_coverage_profile(root: Path, profile: Path, module: str) -> dict[str, list[CoverageBlock]]:
    blocks: dict[str, list[CoverageBlock]] = {}
    for line in profile.read_text(encoding="utf-8").splitlines():
        if line.startswith("mode:"):
            continue
        match = COVER_PROFILE_RE.match(line)
        if not match:
            continue

        path = normalize_profile_path(match.group(1), root, module)
        if path is None or not is_backend_production_go_path(path):
            continue

        start_line = int(match.group(2))
        end_line = int(match.group(3))
        count = int(match.group(4))
        blocks.setdefault(path, []).append(CoverageBlock(start_line, end_line, count))
    return blocks


def overlapping_blocks(blocks: list[CoverageBlock], line_number: int) -> list[CoverageBlock]:
    return [block for block in blocks if block.start_line <= line_number <= block.end_line]


def check_diff_coverage(
    added_lines: dict[str, set[int]], coverage_blocks: dict[str, list[CoverageBlock]], threshold: float
) -> int:
    covered = 0
    coverable = 0
    uncovered: list[tuple[str, int]] = []

    for path in sorted(added_lines):
        blocks = coverage_blocks.get(path, [])
        for line_number in sorted(added_lines[path]):
            line_blocks = overlapping_blocks(blocks, line_number)
            if not line_blocks:
                continue

            coverable += 1
            # With -coverpkg, the same file can appear once per test package.
            # A line is covered if any package's test run executed it.
            if any(block.count > 0 for block in line_blocks):
                covered += 1
            else:
                uncovered.append((path, line_number))

    if coverable == 0:
        print("No coverable added backend production Go lines found.")
        return 0

    percent = covered / coverable * 100
    print(
        f"Backend diff coverage: {covered}/{coverable} coverable added Go lines "
        f"covered ({percent:.1f}%, required {threshold:.1f}%)."
    )

    if percent >= threshold:
        return 0

    print("Uncovered added lines:", file=sys.stderr)
    for path, line_number in uncovered[:50]:
        print(f"  {path}:{line_number}", file=sys.stderr)
    if len(uncovered) > 50:
        print(f"  ... and {len(uncovered) - 50} more", file=sys.stderr)
    return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Require backend Go diff coverage for staged production code changes."
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"minimum required diff coverage percentage (default: {DEFAULT_THRESHOLD:g})",
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=DEFAULT_PROFILE,
        help=f"coverage profile path (default: {DEFAULT_PROFILE})",
    )
    parser.add_argument(
        "--compare-ref",
        help="compare working tree against this git ref instead of checking staged changes",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        root = git_root()
        staged = args.compare_ref is None
        diff = run(diff_args(args.compare_ref), cwd=root, capture=True)
        if diff.returncode != 0:
            raise CoverageError(diff.stderr.strip() or "failed to read git diff")

        added_lines = parse_added_lines(diff.stdout)
        added_lines = filter_meaningful_added_lines(root, added_lines, staged=staged)
        if not added_lines:
            print("No added backend production Go lines to check.")
            return 0

        profile = args.profile if args.profile.is_absolute() else root / args.profile
        run_backend_tests(root, profile)
        coverage_blocks = parse_coverage_profile(root, profile, module_path(root))
        return check_diff_coverage(added_lines, coverage_blocks, args.threshold)
    except CoverageError as error:
        print(f"backend diff coverage: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
