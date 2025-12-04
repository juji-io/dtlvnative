# Change Log

## WIP
### Added
- Tight integration of usearch with DLMDB with a WAL/checkpoint approach to
  ensure ACID of vector index.
- Bundle OpenMP in the jar so users don't have to install it.

## 0.13.23
### Added
- Key range full value iterator
### Changed
- Simplify list sampler

## 0.13.21
### Added
- Add the missing key only sampler
### Changed
- Drop Intel macOS build

## 0.13.20
### Added
- Sample iterators based on fast rank API of DLMDB

## 0.13.0
### Changed
- Switch to use DLMDB

## 0.11.4
### Changed
- Use JavaCPP. Removed static compilation as we don't use GraalVM specific
  binding any more. lmdb is compiled as static, and linked into dtlv shared
  library. Platform naming follows JavaCPP as well.

## 0.9.7
### Changed
- actually need apt update

## 0.9.6
### Changed
- don't apt update in cirrus build, otherwise it may get stuck waiting for user input

## 0.9.5
### Added
- Linux aarch64 shared and static build on native platform
### Removed
- Linux aarch64 shared zig build

## 0.9.2
### Changed
- update LMDB to latest

## 0.9.1
### Added
- zig cross compilation for shared libraries on Windows/x86_64 and Linux/aarch64

## 0.8.9
### Changed
- update LMDB to latest

## 0.8.8
### Fix
- forgot to commit

## 0.8.7
### Fix
- fix directory

## 0.8.6
### Changed
- use ubuntu-20.04

## 0.8.5
### Changed
- LMDB tracks mdb.master branch on openldap repo

## 0.8.4
### Changed
- also build static with musl for linux for master branch of LMDB

## 0.7.12
### Changed
- copy musl prebuild libstdc++.a to musl lib dir

## 0.7.11
### Fix
- use $(RM) in make clean

## 0.7.10
### Fix
- make clean error when there's no prior build

## 0.7.9
### Changed
- build shared with gcc on linux

## 0.7.8
### Changed
- build static with musl on linux

## 0.8.3
### Changed
- comparator treats address identity as equal

## 0.8.2
### Fixed
- Windows build

## 0.8.1
### Added
- Shared library build for LMDB on all platforms
### Changed
- Switch to master branch of LMDB

## 0.7.6
### Improved
- tweak gcc option

## 0.7.5
### Improved
- faster comparator

## 0.6.5
### Fixed
- release workflow

## 0.6.4
### Added
- MacOS aarch64 platform. Build both static version for native dtlv, and shared library version for LMDBJava
### Changed
Update LMDB to 0.9.29, following LMDBJava

## 0.5.0
### Added
- comparator for dupsort

## 0.4.0
### Changed
- revert shared libraries
- put platform specific dirs under dtlvnative

## 0.3.0
### Changed
- compile shared libraries

## 0.2.0
### Changed
- put libs under platform specific dir

## 0.1.10
### Added
- New project
