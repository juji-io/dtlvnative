# dtlvnative

Provides pre-built native dependencies for
[Datalevin](https://github.com/juji-io/datalevin) database. This is done by
packaging the compiled shared C libraries and JavaCPP library files in the
platform specific JAR files.

The following two shared libraries are included as resources in the JAR:

* `dtlv `shared library wraps LMDB library, and implements comparator, iterators
  and samplers.
* `jniDTLV` shared library includes helpers for JavaCPP to wrap dtlv using JNI.

The following platforms are currently supported:

* macosx-arm64
* macosx-x86_64
* linux-arm64
* linux-x86_64
* windows-x86-64

## License

Copyright © 2021-2025 Juji, Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
