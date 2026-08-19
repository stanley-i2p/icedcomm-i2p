fn main() {
    println!("cargo:rerun-if-changed=assets/commtools-i2p.ico");

    #[cfg(target_os = "windows")]
    {
        let mut resource = winres::WindowsResource::new();
        resource.set_icon("assets/commtools-i2p.ico");
        resource
            .compile()
            .expect("failed to embed Windows application icon");
    }
}
