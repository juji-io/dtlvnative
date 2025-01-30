# dtlvnative

Provides pre-built native dependencies for
[Datalevin](https://github.com/juji-io/datalevin) database. This is done by
packaging the compiled native libraries and JavaCPP JNI library files in the
platform specific JAR files.

In addition to JavaCPP's JNI library, these native libraries areincluded:

* `lmdb` LMDB key value library.
* `usearch` Vector indexing and similartiy search libary.
* `dtlv `wraps LMDB, and implements Datalevin comparator, iterators and samplers.

The following platforms are currently supported:

* macosx-arm64
* macosx-x86_64
* linux-arm64
* linux-x86_64
* windows-x86-64

The name of the released JAR is `org.clojars.huahaiy/dtlvnative-PLATFORM`
where `PLATFORM` is one of the above.

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
