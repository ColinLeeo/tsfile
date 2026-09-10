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

# Native Development Package Artifact Workflow Design

## Context

Apache TsFile has a multi-platform Python wheel workflow and an open native
packaging change in PR #914, but it does not yet have one workflow that builds
publishable C++ package artifacts for Linux, macOS, and Windows. The first
iteration will run in the `ColinLeeo/tsfile` fork so the artifacts can be
examined before any publishing automation is added to the Apache repository.

The work is based on the latest `colin/develop`. The three native packaging
commits from `feature/cpp-packaging` are applied to that base without changing
the original PR branch.

## Goals

- Add one manually triggered workflow that produces all native development
  package artifacts from one commit and one generated development version.
- Build DEB, RPM, Homebrew Bottle, and Windows ZIP artifacts.
- Validate both the command-line tool and the installed C++ SDK.
- Produce one final artifact bundle with checksums and a machine-readable
  manifest only when every required platform succeeds.
- Make the resulting files ready for a later, separate upload to the JFrog
  `dev` channels.

## Non-goals

- Uploading anything to JFrog.
- Updating a mutable `latest` pointer.
- Creating Git tags or GitHub Releases.
- Producing RC or final release versions.
- Signing packages or repository metadata.
- Publishing through Homebrew Core, Winget, Chocolatey, or NuGet.
- Adding Linux ARM64, Windows ARM64, or Linuxbrew in the first iteration.

## Workflow Interface

The workflow is stored at `.github/workflows/native-packages.yml` and exposes
only `workflow_dispatch`. It has read-only repository permissions and uses no
publishing credentials.

"Manually triggered" refers only to a maintainer selecting **Run workflow** in
GitHub Actions. All compilation, packaging, installation tests, checksums, and
artifact assembly after that click are automated.

The caller does not enter a version. A `prepare` job reads
`TsFile_CPP_VERSION` from `cpp/CMakeLists.txt`, requires the form
`MAJOR.MINOR.PATCH.dev`, and derives one immutable build identity from:

- the base semantic version;
- the UTC build date;
- `github.run_number`;
- `github.run_attempt`;
- the short Git commit SHA.

For a base version of `2.5.0`, an example identity is:

```text
logical:  2.5.0.dev0+20260910.123.1.gabcdef1
deb:      2.5.0~dev0+20260910.123.1.gabcdef1-1
rpm:      Version 2.5.0, Release 0.dev0.20260910.123.1.gabcdef1.el9
archive:  2.5.0-dev0.20260910.123.1.gabcdef1
homebrew: 2.5.0.dev0.20260910.123.1.gabcdef1
```

The workflow fails before building if the source version cannot be parsed.

## Build Matrix

| Family | Build environment | Architecture | Output |
| --- | --- | --- | --- |
| DEB | Ubuntu 22.04 | amd64 | `tsfile`, `tsfile-dev`, `tsfile-tools` |
| RPM | AlmaLinux 9 | x86_64 | `tsfile`, `tsfile-devel`, `tsfile-tools` |
| Homebrew | current ARM macOS runner | arm64 | `tsfile-dev` Bottle and JSON |
| Homebrew | macOS 15 Intel runner | x86_64 | `tsfile-dev` Bottle and JSON |
| Windows | Windows Server 2022, MSVC v143 | x86_64 | combined CLI and SDK ZIP |

The Linux package name `tsfile-tools` is intentionally retained. It contains
the `tsfile-cli` executable and depends on the `tsfile` runtime package.

The DEB artifacts are built once on the oldest supported Ubuntu baseline and
then installed and tested on clean Ubuntu 22.04 and 24.04 environments. This
avoids publishing two different binaries with the same package identity.

## CMake and CPack Version Interface

The native packaging configuration gains explicit cache variables for
generator-specific versions. Defaults preserve local developer behavior, while
the workflow passes generated values for each package family:

- a common archive version;
- a complete Debian package version;
- an RPM Version and Release pair.

Component dependency declarations use the complete generator-specific version,
so `tsfile-dev`, `tsfile-devel`, and `tsfile-tools` always require the matching
runtime build. CPack-generated external filenames use native package naming
instead of the current component-style RPM filenames.

## Job Design

### `prepare`

Checks out the requested commit, computes all version representations, validates
them, and uploads a small version manifest for downstream jobs. Every build job
uses these outputs rather than recomputing a version independently.

### `build-deb`

Builds Release C++ libraries and tools on Ubuntu 22.04 with native architecture
optimizations disabled, runs CPack's DEB generator, inspects package metadata,
and verifies the three expected package names and dependency relationship. It
uploads the DEBs as an intermediate artifact.

Two downstream test jobs install the DEBs with the distribution package manager
on clean Ubuntu 22.04 and 24.04. They run `tsfile-cli --version`, compile and run
a small C++ consumer through installed CMake package metadata, and fail on any
missing library, header, executable, or dependency.

### `build-rpm`

Builds on AlmaLinux 9, uses CPack's RPM generator, and verifies the three native
package names and their exact Version/Release metadata. A clean AlmaLinux 9 test
job installs the RPMs through DNF, runs the CLI check, and compiles and runs the
same installed C++ consumer contract.

### `build-homebrew`

The ARM64 and Intel jobs download the immutable GitHub source archive for the
exact fork commit and create a temporary local tap containing `tsfile-dev.rb`.
The formula builds that archive as a Bottle candidate, runs its CLI and C++
consumer tests, and calls `brew bottle --json` with the future immutable JFrog
root URL. The development Formula is keg-only because the installed SDK includes
its current dependency header closure, which must not be linked over files owned
by Homebrew dependencies such as `simde`.

A macOS merge job consumes both Bottle JSON files and produces the final
`tsfile-dev.rb` with one `bottle do` block containing both platform checksums.
The formula's source URL identifies the exact fork commit. Its Bottle root URL
has this shape:

```text
https://packages.apache.org/artifactory/tsfile/homebrew/dev/versions/<homebrew-version>/bottles
```

This workflow does not upload to that URL; it only makes the formula and bottle
files mutually consistent for a later publishing step.

### `build-windows`

Builds a Release configuration with the Visual Studio 2022 generator and v143
x64 toolset. It installs into a staging prefix and produces one ZIP containing:

- `tsfile-cli.exe`;
- the TsFile DLL and MSVC import library;
- public headers;
- CMake package files;
- Apache LICENSE and NOTICE files;
- any non-system runtime DLLs required by the CLI.

The job tests the CLI from the staged prefix and configures, compiles, and runs a
small MSVC C++ project using the staged `TsFileConfig.cmake`. The ZIP therefore
supports both command-line users and C++ SDK consumers.

### `assemble`

This job depends on every build and install-test job and runs only when all are
successful. It downloads the intermediate artifacts, arranges them by package
family and platform, generates `SHA256SUMS`, and writes `manifest.json` with the
source commit, generated versions, platforms, filenames, sizes, and checksums.

It uploads one final GitHub Actions artifact named
`tsfile-native-packages-<archive-version>`, with a retention period of 14 days.
The local assembly directory uses the logical version of the same build identity.

The manifest also records the intended JFrog repository, immutable target path,
and Debian coordinates for each file. This does not publish anything, but it
removes guesswork from the later manual publishing and testing step.

## Boundary with JFrog Publishing

JFrog repository administration is not repeated for every package build.
Repository-level behavior is configured once: Debian indexing must be enabled,
and the RPM repository must have automatic metadata calculation and the folder
depth required by the selected channel/OS/architecture layout.

After downloading a successful Actions artifact, a maintainer performs a
separate publishing step:

- upload each DEB with its `deb.distribution`, `deb.component`, and
  `deb.architecture` coordinates from the manifest;
- upload RPMs under the manifest's `dev/el9/x86_64` metadata root;
- upload Homebrew Bottles and the immutable Formula under the manifest's
  `versions/<version>` path;
- install through APT, DNF, and Homebrew to verify the public repository view;
- update the Homebrew `latest` Formula only after those checks pass.

This first iteration performs that publishing step manually. A later workflow
can consume the same manifest and automate it without changing the build jobs.

## Final Artifact Layout

```text
tsfile-native-packages-<logical-version>/
├── manifest.json
├── SHA256SUMS
├── deb/
│   └── ubuntu22.04-amd64/
├── rpm/
│   └── almalinux9-x86_64/
├── homebrew/
│   ├── Formula/tsfile-dev.rb
│   └── bottles/
└── windows/
    └── tsfile-<archive-version>-windows-x86_64.zip
```

## Failure and Safety Behavior

- Matrix jobs use `fail-fast: false` so all platform failures are visible in one
  run.
- Validation commands do not use `|| true` for required checks.
- Failed jobs may leave their intermediate Actions artifacts for diagnosis, but
  `assemble` does not run and no complete bundle is produced.
- Artifact paths are explicit and do not use broad recursive globs outside the
  package staging directories.
- The workflow has no write permission and no external credentials.
- Published version immutability is enforced later by the separate JFrog
  workflow; this workflow guarantees each run already has a unique identity.

## Verification

Before the workflow is pushed to the fork default branch:

- validate YAML syntax and GitHub Actions expressions;
- test version parsing and ordering with representative inputs;
- configure CPack locally and inspect generated package configuration;
- build and install the native macOS archive locally where practical;
- verify all new files have Apache license headers;
- run the repository's formatting and relevant C++ build checks.

The first manual run on `colin/develop` is the end-to-end acceptance test. Its
final artifact bundle must contain every matrix output and pass all installation
jobs before any file is manually uploaded to JFrog.
