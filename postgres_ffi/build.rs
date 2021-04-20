extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to invalidate the built crate whenever the wrapper changes
    println!("cargo:rerun-if-changed=pg_control_ffi.h");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("pg_control_ffi.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks))

        .whitelist_type("ControlFileData")
        .whitelist_var("PG_CONTROL_FILE_SIZE")
        .whitelist_var("PG_CONTROLFILEDATA_OFFSETOF_CRC")
        .whitelist_type("DBState")

    // Path the server include dir. It is in tmp_install/include/server, if you did
    // "configure --prefix=<path to tmp_install>". But if you used "configure --prefix=/",
    // and used DESTDIR to move it into tmp_install, then it's in
    // tmp_install/include/postgres/server (that's how the pgbuild.sh script does it).
    // 'pg_config --includedir-server' would perhaps be the more proper way to find it,
    // but this will do for now.
        .clang_arg("-I../tmp_install/include/server")
        .clang_arg("-I../tmp_install/include/postgresql/server")

        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}