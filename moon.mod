// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "Ycq-68798/moon-flight-sql"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/Ycq-68798/moon-flight-sql"

license = "Apache-2.0"

keywords = [ ]

preferred_target = "native"

description = "Native MoonBit Arrow Flight SQL query client"

import {
  "moonbitlang/async@0.22.5",
  "moonbitstack/moonrpc@0.21.0",
  "shunge/arrow@0.2.1",
}
