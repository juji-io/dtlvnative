# dtlvnative

Provides pre-built native dependencies for
[Datalevin](https://github.com/juji-io/datalevin) database. This is done by
packaging the compiled static or shared C libraries and header files in JAR
files.

Three types of libraries are built here.

1. Static libraries. These are used in GraalVM native image version of
   Datalevin and are built using the musl tool chain.

2. Shared libraries built on native platforms. These are built using native tool
   chains of the respective platform due to good availability of these platforms
   on free public CI/CD platforms. These are under directory `*-shared`.

3. Cross compiled shared libraries. These are built using zig build tool. These
   are under directory matching zig target names.

## License

Copyright © 2021-2024 Juji, Inc.

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
