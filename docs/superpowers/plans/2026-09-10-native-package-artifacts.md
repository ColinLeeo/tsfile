<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Native Development Package Artifacts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one manually triggered GitHub Actions workflow that builds and validates immutable TsFile C++ development package artifacts for Linux, macOS, and Windows without publishing them.

**Architecture:** A `prepare` job generates one build identity and package-manager-specific versions. Parallel platform jobs build native packages and prove the installed CLI and C++ SDK work; a final job emits one checksummed artifact bundle and a JFrog-ready manifest only when all required jobs pass.

**Tech Stack:** GitHub Actions, CMake/CPack, DEB/dpkg, RPM/DNF, Homebrew Bottle tooling, MSVC, Python 3 standard library.

**Spec:** `docs/superpowers/specs/2026-09-10-native-package-artifacts-design.md`

## Global Constraints

- Work from the latest `colin/develop` in `/Users/colin/dev/tsfile/worktrees/native-package-artifacts`; do not change `feature/cpp-packaging` or the user's original checkout.
- Preserve the Linux package names `tsfile`, `tsfile-dev`/`tsfile-devel`, and `tsfile-tools`; `tsfile-tools` contains `tsfile-cli`.
- The workflow trigger is only `workflow_dispatch`, with `contents: read` and no secrets.
- The first version produces development builds only; it reads `MAJOR.MINOR.PATCH.dev` from `cpp/CMakeLists.txt` and has no version input.
- Every run has a unique identity containing UTC date, run number, run attempt, and short commit SHA.
- Required outputs are Ubuntu-compatible DEBs, AlmaLinux 9 RPMs, ARM64 and Intel Homebrew Bottles plus Formula, and an MSVC x64 Windows CLI/SDK ZIP.
- No JFrog upload, mutable `latest` update, signing, tag, GitHub Release, RC, or final release behavior is added.
- A final bundle is created only after every required build and installation test succeeds; intermediate artifacts expire after 14 days.
- Every new source, script, workflow, and Markdown file carries the Apache License 2.0 header in the appropriate comment style.

---

### Task 1: Bring the Native Packaging Foundation onto the Fork Base

**Files:**
- Modify through cherry-pick: `cpp/CMakeLists.txt`
- Modify through cherry-pick: `cpp/src/CMakeLists.txt`
- Modify through cherry-pick: `cpp/tools/CMakeLists.txt`
- Create through cherry-pick: `cpp/cmake/TsFileConfig.cmake.in`
- Create through cherry-pick: `cpp/cmake/TsFilePublicHeaders.cmake`
- Create through cherry-pick: `cpp/cmake/tsfile.pc.in`
- Create through cherry-pick: `packaging/README.md`
- Create through cherry-pick: `packaging/homebrew/tsfile.rb`
- Create through cherry-pick: `.github/workflows/cpp-packaging.yml`

**Interfaces:**
- Consumes: `colin/develop` at the worktree base and commits `0edf0cfde`, `067ebd4c4`, `914a23d8f` from `feature/cpp-packaging`.
- Produces: working `cmake --install` and CPack DEB/RPM integration on the fork's current C++ version `2.5.0.dev`.

- [ ] **Step 1: Apply the three reviewed packaging commits in order**

Run:

```bash
git cherry-pick 0edf0cfde 067ebd4c4 914a23d8f
```

Expected: all three commits apply, or conflicts are limited to files changed on
`colin/develop` since the PR base.

- [ ] **Step 2: Resolve version conflicts without regressing the fork**

Keep this exact source version in `cpp/CMakeLists.txt`:

```cmake
set(TsFile_CPP_VERSION 2.5.0.dev)
```

Retain all newer commits already present on `colin/develop`. Do not resolve a
conflict by replacing a whole file with the older PR version.

- [ ] **Step 3: Verify the install and packaging configuration exists**

Run:

```bash
cmake -S cpp -B /tmp/tsfile-package-foundation \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TEST=OFF \
  -DBUILD_TOOLS=ON \
  -DTSFILE_ENABLE_CPACK=ON \
  -DTSFILE_DEPENDENCY_SOURCE=AUTO \
  -DTSFILE_ENABLE_NATIVE_ARCH=OFF
```

Expected: configuration succeeds and
`/tmp/tsfile-package-foundation/CPackConfig.cmake` exists.

- [ ] **Step 4: Run the focused formatting and license checks**

Run:

```bash
./mvnw -P with-cpp -DskipTests -Dbuild.test=OFF verify
```

Expected: Maven reports `BUILD SUCCESS`.

- [ ] **Step 5: Record the imported commit sequence**

Run:

```bash
git log --oneline --max-count=5
```

Expected: the three packaging changes appear above the design commit. No extra
commit is needed because cherry-pick creates the integration commits.

### Task 2: Add Deterministic Development Version Generation

**Files:**
- Create: `packaging/scripts/native_package_versions.py`
- Create: `packaging/tests/test_native_package_versions.py`
- Modify: `cpp/CMakeLists.txt`

**Interfaces:**
- Consumes: `set(TsFile_CPP_VERSION MAJOR.MINOR.PATCH.dev)` in `cpp/CMakeLists.txt`.
- Produces: `read_cpp_version(path: Path) -> str`, `build_versions(source_version: str, build_date: str, run_number: int, run_attempt: int, git_sha: str) -> dict[str, str]`, and a CLI that writes JSON plus optional GitHub output variables.
- Produces CMake cache variables `TSFILE_ARCHIVE_VERSION`, `TSFILE_DEBIAN_PACKAGE_VERSION`, `TSFILE_RPM_PACKAGE_VERSION`, and `TSFILE_RPM_PACKAGE_RELEASE`.

- [ ] **Step 1: Write the version unit tests**

Create tests covering the exact successful mapping:

```python
self.assertEqual(
    build_versions("2.5.0.dev", "20260910", 123, 1, "abcdef123456"),
    {
        "base_version": "2.5.0",
        "logical_version": "2.5.0.dev0+20260910.123.1.gabcdef1",
        "deb_version": "2.5.0~dev0+20260910.123.1.gabcdef1-1",
        "rpm_version": "2.5.0",
        "rpm_release": "0.dev0.20260910.123.1.gabcdef1.el9",
        "archive_version": "2.5.0-dev0.20260910.123.1.gabcdef1",
        "homebrew_version": "2.5.0.dev0.20260910.123.1.gabcdef1",
    },
)
```

Load `packaging/scripts/native_package_versions.py` with
`importlib.util.spec_from_file_location` so the repository's `packaging/`
directory cannot be confused with the third-party Python `packaging` module.

Also assert that `2.5.0`, `2.5.dev`, non-numeric run fields, malformed dates,
and SHAs shorter than seven hexadecimal characters raise `ValueError`.

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 packaging/tests/test_native_package_versions.py -v
```

Expected: FAIL because `native_package_versions` does not exist.

- [ ] **Step 3: Implement the version helper and CLI**

Implement strict regular expressions, take the first seven lowercase SHA
characters, and expose:

```bash
python3 packaging/scripts/native_package_versions.py \
  --cmake-file cpp/CMakeLists.txt \
  --build-date 20260910 \
  --run-number 123 \
  --run-attempt 1 \
  --git-sha abcdef123456 \
  --json-out versions.json \
  --github-output "$GITHUB_OUTPUT"
```

Write each dictionary key as `key=value` to the optional GitHub output file.

- [ ] **Step 4: Run the unit tests**

Run:

```bash
python3 packaging/tests/test_native_package_versions.py -v
```

Expected: all version parsing, formatting, and rejection tests PASS.

- [ ] **Step 5: Add generator-specific CPack overrides**

In `cpp/CMakeLists.txt`, define cache strings with safe defaults:

```cmake
set(TSFILE_ARCHIVE_VERSION "${TSFILE_PACKAGE_VERSION}" CACHE STRING
    "Version used in portable archive names")
set(TSFILE_DEBIAN_PACKAGE_VERSION "${TSFILE_PACKAGE_VERSION}" CACHE STRING
    "Complete Debian package version")
set(TSFILE_RPM_PACKAGE_VERSION "${TSFILE_PACKAGE_VERSION}" CACHE STRING
    "RPM Version value")
set(TSFILE_RPM_PACKAGE_RELEASE "1" CACHE STRING
    "RPM Release value")
```

Set `CPACK_PACKAGE_VERSION` from `TSFILE_ARCHIVE_VERSION`,
`CPACK_DEBIAN_PACKAGE_VERSION` from `TSFILE_DEBIAN_PACKAGE_VERSION`, and the RPM
Version/Release fields from their respective variables. Use the complete DEB or
RPM version in component dependency declarations. Add
`CPACK_RPM_FILE_NAME RPM-DEFAULT` so the external filenames use native names.

- [ ] **Step 6: Prove CPack receives exact versions**

Run a configure into `/tmp/tsfile-package-version-test` with:

```bash
-DTSFILE_ARCHIVE_VERSION=2.5.0-dev0.20260910.123.1.gabcdef1
-DTSFILE_DEBIAN_PACKAGE_VERSION=2.5.0~dev0+20260910.123.1.gabcdef1-1
-DTSFILE_RPM_PACKAGE_VERSION=2.5.0
-DTSFILE_RPM_PACKAGE_RELEASE=0.dev0.20260910.123.1.gabcdef1.el9
```

Inspect `CPackConfig.cmake` and assert all four literal values are present.

- [ ] **Step 7: Commit the version contract**

```bash
git add cpp/CMakeLists.txt packaging/scripts/native_package_versions.py packaging/tests/test_native_package_versions.py
git commit -m "feat(ci): add native package development versions"
```

### Task 3: Add Linux Package Build and Installation Jobs

**Files:**
- Create: `.github/workflows/native-packages.yml`
- Modify: `packaging/README.md`

**Interfaces:**
- Consumes: version helper outputs from Task 2 and CPack install/package targets from Task 1.
- Produces: intermediate artifact `native-deb-ubuntu22.04-amd64` with three DEBs and `native-rpm-almalinux9-x86_64` with three RPMs.
- Produces jobs `prepare`, `build-deb`, `test-deb`, `build-rpm`, and `test-rpm` for later `assemble.needs`.

- [ ] **Step 1: Add the manual trigger and prepare job**

The workflow header must be:

```yaml
name: Build native package artifacts

on:
  workflow_dispatch:

permissions:
  contents: read
```

Checkout with full history, run the version helper with
`date -u +%Y%m%d`, `${{ github.run_number }}`, `${{ github.run_attempt }}`, and
`${{ github.sha }}`, expose every generated version as a job output, and upload
`versions.json` for final assembly.

- [ ] **Step 2: Add the Ubuntu 22.04 DEB build**

Install `build-essential`, `cmake`, `ninja-build`, `pkg-config`, `dpkg-dev`, and
`uuid-dev`; configure Release CPack using the prepare job's archive and Debian
versions; build; run `cpack -G DEB`; and assert exactly these package names:

```text
tsfile
tsfile-dev
tsfile-tools
```

Use `dpkg-deb --field` to check Package and Version, and upload only `*.deb`.

- [ ] **Step 3: Add clean DEB installation tests**

Create a `test-deb` matrix with `ubuntu:22.04` and `ubuntu:24.04` containers.
Download the DEB artifact and install all three packages with APT. Assert:

```bash
tsfile-cli --version
dpkg-query -W tsfile tsfile-dev tsfile-tools
```

Generate a minimal CMake project that uses:

```cmake
find_package(TsFile CONFIG REQUIRED)
add_executable(installed_consumer main.cpp)
target_link_libraries(installed_consumer PRIVATE TsFile::tsfile)
```

The `main.cpp` includes `<tsfile/cwrapper/tsfile_cwrapper.h>` and returns success
only when `TS_DATATYPE_INT32 == 1`. Configure, build, and run it.

- [ ] **Step 4: Add the AlmaLinux 9 RPM build**

Run on `ubuntu-24.04` with `container: almalinux:9`. Install `cmake`, `gcc-c++`,
`ninja-build`, `pkgconf-pkg-config`, `rpm-build`, `git`, `curl`, `tar`, `gzip`,
and `libuuid-devel`. Configure with the prepare job's archive, RPM Version, and
RPM Release outputs; build and run `cpack -G RPM`.

Use `rpm -qp` query formats to assert names `tsfile`, `tsfile-devel`, and
`tsfile-tools`, plus the exact Version/Release. Upload only `*.rpm`.

- [ ] **Step 5: Add a clean RPM installation test**

Use a fresh `almalinux:9` container, download the RPM artifact, install with:

```bash
dnf install -y packages/*.rpm cmake gcc-c++
```

Run `tsfile-cli --version`, query all three installed packages, and compile/run
the same `find_package(TsFile CONFIG REQUIRED)` consumer.

- [ ] **Step 6: Validate workflow structure locally**

Run:

```bash
ruby -e 'require "yaml"; YAML.safe_load_file(".github/workflows/native-packages.yml", aliases: true)'
rg -n 'workflow_dispatch|contents: read|build-deb|test-deb|build-rpm|test-rpm' .github/workflows/native-packages.yml
```

Expected: YAML parses and every required job marker is found.

- [ ] **Step 7: Commit the Linux workflow slice**

```bash
git add .github/workflows/native-packages.yml packaging/README.md
git commit -m "feat(ci): build native Linux package artifacts"
```

### Task 4: Add the Windows CLI and SDK ZIP

**Files:**
- Modify: `.github/workflows/native-packages.yml`
- Modify: `cpp/CMakeLists.txt`
- Modify: `packaging/README.md`

**Interfaces:**
- Consumes: `archive_version` and the CMake install export.
- Produces: job `build-windows` and intermediate artifact `native-windows-msvc-x86_64` containing one combined ZIP.

- [ ] **Step 1: Add archive configuration assertions**

Document and verify that the CPack ZIP generator uses one combined archive by
leaving `CPACK_ARCHIVE_COMPONENT_INSTALL` disabled. Set an explicit package file
base for Windows with the archive version and `windows-x86_64` suffix.

- [ ] **Step 2: Add the Windows build job**

Use `windows-2022`, `ilammy/msvc-dev-cmd@v1` with `arch: x64`, CMake's
`Visual Studio 17 2022` generator, `-A x64`, Release configuration, bundled
dependencies, tools enabled, native architecture disabled, and CPack enabled.

Build with:

```powershell
cmake --build build/windows --config Release --parallel
cmake --install build/windows --config Release --prefix stage/windows
```

- [ ] **Step 3: Test staged CLI and SDK**

Add `stage/windows/bin` to `PATH`, run `tsfile-cli.exe --version`, then generate
the same consumer CMake project. Configure it with
`-DCMAKE_PREFIX_PATH=<absolute stage/windows path>`, build Release, and execute
the consumer. Fail if the DLL, import library, CLI, CMake config, LICENSE, or
NOTICE is absent.

- [ ] **Step 4: Generate and inspect the ZIP**

Run CPack's ZIP generator with the archive version override. Use PowerShell's
archive inspection to assert paths ending in:

```text
bin/tsfile-cli.exe
bin/tsfile.dll
lib/tsfile.lib
include/tsfile/cwrapper/tsfile_cwrapper.h
```

Use the actual CMake target output names if MSVC produces a differently cased
DLL or import library, and make the installation contract consistent rather
than renaming only inside the test.

- [ ] **Step 5: Upload and validate workflow YAML**

Upload the single ZIP as `native-windows-msvc-x86_64` with 14-day retention,
then run the Ruby YAML parse command from Task 3.

- [ ] **Step 6: Commit the Windows package**

```bash
git add .github/workflows/native-packages.yml cpp/CMakeLists.txt packaging/README.md
git commit -m "feat(ci): build Windows native SDK archive"
```

### Task 5: Add Homebrew Dev Formula and Bottle Jobs

**Files:**
- Create: `packaging/homebrew/tsfile-dev.rb.in`
- Create: `packaging/scripts/render_homebrew_formula.py`
- Create: `packaging/tests/test_render_homebrew_formula.py`
- Modify: `.github/workflows/native-packages.yml`
- Modify: `packaging/README.md`

**Interfaces:**
- Consumes: `homebrew_version`, exact repository/SHA, and per-platform Bottle JSON.
- Produces: `render_formula(template: str, values: dict[str, str], bottle_block: str = "") -> str` and final `Formula/tsfile-dev.rb`.
- Produces: ARM64 and x86_64 Bottle tarballs and JSON metadata under `homebrew/bottles/`.

- [ ] **Step 1: Write Formula rendering tests**

Test that rendering fills exact immutable values:

```ruby
class TsfileDev < Formula
  url "https://github.com/ColinLeeo/tsfile/archive/<full-sha>.tar.gz"
  version "2.5.0.dev0.20260910.123.1.gabcdef1"
  sha256 "<source-sha256>"
  bottle do
    root_url "https://packages.apache.org/artifactory/tsfile/homebrew/dev/versions/2.5.0.dev0.20260910.123.1.gabcdef1/bottles"
  end
end
```

Load `packaging/scripts/render_homebrew_formula.py` by filesystem path with
`importlib.util`, following the same isolation rule as the version tests.

Assert missing placeholders, duplicate bottle blocks, and an empty source SHA
are rejected.

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 packaging/tests/test_render_homebrew_formula.py -v
```

Expected: FAIL because the renderer does not exist.

- [ ] **Step 3: Implement the Formula template and renderer**

The template retains the existing dependencies and CMake install behavior,
uses class `TsfileDev`, formula name `tsfile-dev`, the exact commit archive, and
the C++ consumer plus `tsfile-cli --version` test. The renderer replaces only
declared `@TOKEN@` values and fails if any `@[A-Z0-9_]+@` token remains.

- [ ] **Step 4: Run renderer tests and Ruby syntax validation**

Run:

```bash
python3 packaging/tests/test_render_homebrew_formula.py -v
ruby -c /tmp/rendered-tsfile-dev.rb
```

Expected: unit tests pass and Ruby reports `Syntax OK` for the test fixture.

- [ ] **Step 5: Add parallel Homebrew Bottle jobs**

Add matrix entries matching the wheel workflow's proven runners:

```yaml
- name: macos-arm64
  os: macos-latest
- name: macos-x86_64
  os: macos-15-intel
```

Each job downloads the exact commit archive, computes its SHA-256, creates a
temporary local tap, renders the pre-bottle Formula, installs with
`brew install --build-bottle`, runs `brew test`, and executes:

```bash
brew bottle --json --root-url "$BOTTLE_ROOT_URL" apache/tsfile-dev/tsfile-dev
```

Upload the generated Bottle and JSON as a platform-specific intermediate
artifact.

- [ ] **Step 6: Add the Bottle merge job**

On a macOS runner, download both Bottle artifacts, recreate the local tap with
the same pre-bottle Formula, and merge both JSON files with:

```bash
brew bottle --merge --write --no-commit homebrew/bottles/*.json
```

Copy the updated formula to `homebrew/Formula/tsfile-dev.rb`. Assert it contains
the configured JFrog `root_url`, both generated platform tags, and no template
tokens. Upload Formula, Bottle tarballs, and JSON as `native-homebrew`.

- [ ] **Step 7: Commit Homebrew artifact generation**

```bash
git add .github/workflows/native-packages.yml packaging/homebrew/tsfile-dev.rb.in packaging/scripts/render_homebrew_formula.py packaging/tests/test_render_homebrew_formula.py packaging/README.md
git commit -m "feat(ci): build Homebrew development bottles"
```

### Task 6: Assemble the Checksummed Publishable Bundle

**Files:**
- Create: `packaging/scripts/assemble_native_packages.py`
- Create: `packaging/tests/test_assemble_native_packages.py`
- Modify: `.github/workflows/native-packages.yml`
- Modify: `packaging/README.md`

**Interfaces:**
- Consumes: downloaded intermediate artifact directory plus `versions.json`.
- Produces: `assemble(input_dir: Path, output_dir: Path, versions: dict[str, str], source: dict[str, str]) -> dict[str, object]`, `SHA256SUMS`, and `manifest.json`.
- Produces final job `assemble` and artifact `tsfile-native-packages-<archive-version>`.

- [ ] **Step 1: Write assembly unit tests**

Build a temporary fixture with one representative DEB, RPM, Bottle, Formula,
Bottle JSON, and Windows ZIP. Assert deterministic output paths, sorted
`SHA256SUMS`, byte sizes, lowercase SHA-256 values, and manifest entries with:

```json
{
  "family": "deb",
  "targetRepository": "tsfile-debian",
  "targetPath": "pool/dev/ubuntu22.04-amd64/<filename>",
  "properties": {
    "deb.distribution": ["jammy", "noble"],
    "deb.component": ["dev"],
    "deb.architecture": ["amd64"]
  }
}
```

Load `packaging/scripts/assemble_native_packages.py` by filesystem path with
`importlib.util`; do not add `__init__.py` files that would shadow the external
Python `packaging` library used by other project tooling.

Assert RPM targets use `tsfile-rpm/dev/el9/x86_64/`, Homebrew targets use the
immutable `tsfile/homebrew/dev/versions/<homebrew-version>/` path, Windows uses
`tsfile/windows/dev/versions/<archive-version>/`, and unknown files fail.

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
python3 packaging/tests/test_assemble_native_packages.py -v
```

Expected: FAIL because the assembler does not exist.

- [ ] **Step 3: Implement deterministic assembly**

Use only the Python standard library. Copy files into the design's `deb/`,
`rpm/`, `homebrew/`, and `windows/` directories, compute hashes after copying,
write JSON with `indent=2` and `sort_keys=True`, and sort checksum lines by POSIX
relative path. Reject symlinks, duplicate target paths, empty files, and file
types not declared by the contract.

- [ ] **Step 4: Run assembly tests**

Run:

```bash
python3 packaging/tests/test_assemble_native_packages.py -v
```

Expected: all layout, checksum, metadata, and rejection tests PASS.

- [ ] **Step 5: Add the final workflow job**

Make `assemble` depend on `prepare`, both DEB installation matrix results, RPM
installation, Homebrew merge, and Windows. Download all intermediate artifacts
into explicit subdirectories, call the assembler, verify every manifest hash,
and upload the final directory with 14-day retention. Do not use
`if: always()`; normal successful-needs behavior prevents partial bundles.

- [ ] **Step 6: Commit final assembly**

```bash
git add .github/workflows/native-packages.yml packaging/scripts/assemble_native_packages.py packaging/tests/test_assemble_native_packages.py packaging/README.md
git commit -m "feat(ci): assemble native package artifact bundle"
```

### Task 7: Remove the Superseded Workflow and Perform Final Verification

**Files:**
- Delete: `.github/workflows/cpp-packaging.yml`
- Modify: `packaging/README.md`
- Verify: all files from Tasks 1-6

**Interfaces:**
- Consumes: complete manual artifact workflow.
- Produces: one authoritative native package workflow with no automatic push or pull-request publishing behavior.

- [ ] **Step 1: Remove the old packaging workflow**

Delete `.github/workflows/cpp-packaging.yml` so the fork does not run the older
Ubuntu/Fedora artifact workflow automatically on pushes or pull requests.
Update `packaging/README.md` to name `Build native package artifacts`, explain
manual dispatch, enumerate outputs, and state explicitly that publishing is a
separate step.

- [ ] **Step 2: Run all packaging helper tests**

Run:

```bash
python3 -m unittest discover -s packaging/tests -p 'test_*.py' -v
```

Expected: all version, Formula rendering, and assembly tests PASS.

- [ ] **Step 3: Validate syntax and repository policy**

Run:

```bash
ruby -e 'require "yaml"; YAML.safe_load_file(".github/workflows/native-packages.yml", aliases: true)'
ruby -c packaging/homebrew/tsfile.rb
git diff --check colin/develop...HEAD
./mvnw spotless:check
./mvnw apache-rat:check -P with-cpp
```

Expected: YAML and Ruby parse, no whitespace errors, formatting passes, and RAT
reports zero unapproved files.

- [ ] **Step 4: Run local native build/package smoke tests**

Run the version helper with fixed test inputs, configure CPack with its outputs,
build Release tools, install into `/tmp/tsfile-native-stage`, run the installed
CLI, compile/run the installed consumer, and generate a TGZ package. On the
local macOS host, render the development Formula and run `brew audit --formula`
and `brew style` where they do not require publishing.

- [ ] **Step 5: Review the complete diff**

Run:

```bash
git status --short
git diff --stat colin/develop...HEAD
git diff --check colin/develop...HEAD
git log --oneline --decorate colin/develop..HEAD
```

Expected: only the packaging foundation, native artifact workflow, helpers,
tests, documentation, spec, and plan differ from `colin/develop`.

- [ ] **Step 6: Commit verification-only documentation changes if needed**

If Step 3 or Step 4 required documentation corrections, commit only those
corrections:

```bash
git add packaging/README.md
git commit -m "docs: document native package artifact workflow"
```

If no files changed, do not create an empty commit.

- [ ] **Step 7: Push only after local verification**

Push the tested commit sequence to the fork's `develop` branch without changing
the Apache remote or the original PR branch:

```bash
git push colin HEAD:develop
```

Expected: `ColinLeeo/tsfile` `develop` advances to the verified local HEAD. Then
manually run `Build native package artifacts` in that fork and use the first
complete Actions artifact as the end-to-end acceptance result.
