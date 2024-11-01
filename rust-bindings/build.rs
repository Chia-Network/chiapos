use std::env;
use std::path::PathBuf;

use cmake::Config;

fn main() {
    println!("cargo:rerun-if-changed=../c-bindings/wrapper.h");
    println!("cargo:rerun-if-changed=../c-bindings/wrapper.cpp");
    println!("cargo:rerun-if-changed=../CMakeLists.txt");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());

    let mut cpp_dir = manifest_dir.join("cpp");
    if !cpp_dir.exists() {
        cpp_dir = manifest_dir
            .parent()
            .expect("can't access ../")
            .to_path_buf();
    }

    let dst = Config::new(cpp_dir.as_path())
        .build_target("chiapos_static")
        .define("BUILD_STATIC_CHIAPOS_LIBRARY", "ON")
        .build();

    let blake3_include_path = dst.join("build").join("_deps").join("blake3-src").join("c");

    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build").join("lib").to_str().unwrap()
    );

    println!("cargo:rustc-link-lib=static=chiapos_static");
    println!("cargo:rustc-link-lib=static=blake3");

    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=static=uint128");
    }

    let bindings = bindgen::Builder::default()
        .header(
            cpp_dir
                .join("c-bindings")
                .join("wrapper.h")
                .to_str()
                .unwrap(),
        )
        .clang_arg("-x")
        .clang_arg("c++")
        .clang_arg(format!(
            "-I{}",
            cpp_dir.join("lib").join("include").to_str().unwrap()
        ))
        .clang_arg(format!("-I{}", blake3_include_path.to_str().unwrap()))
        .clang_arg("-std=c++14")
        .allowlist_function("validate_proof")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
