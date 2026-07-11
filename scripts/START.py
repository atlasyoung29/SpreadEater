"""Start SpreadEater in live mode."""

import subprocess, os, shutil, sys

if os.name == "nt":
    cargo_bin = os.path.join(os.path.expanduser("~"), ".cargo", "bin")
    os.environ["PATH"] = cargo_bin + ";" + "C:\\msys64\\mingw64\\bin;" + os.environ.get("PATH", "")
    os.environ["CARGO_TARGET_DIR"] = "C:\\rust-build\\spreadeater"
elif sys.platform == "darwin" and shutil.which("cargo") is None:
    for cargo_dir in ("/opt/homebrew/opt/rustup/bin", os.path.expanduser("~/.cargo/bin")):
        if os.path.isfile(os.path.join(cargo_dir, "cargo")):
            os.environ["PATH"] = cargo_dir + os.pathsep + os.environ.get("PATH", "")
            break

os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

print("Starting SpreadEater (live mode)...")
subprocess.run(["cargo", "run", "--", "live"])
