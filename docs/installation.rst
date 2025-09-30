Installation Guide
==================

Introduction
------------
This guide explains how to install **Hertta**, a GraphQL server written in Rust from the
``predicer-tools/hertta`` repository. The instructions cover Windows and Unix (Linux/macOS) systems,
list all dependencies, and show how to build and run the server.

Prerequisites
-------------
Before cloning the project, you need a few tools installed:

+----------------+------------------------------------------------------------+
| Tool           | Purpose                                                    |
+================+============================================================+
| Rust & Cargo   | Rust compiler and package manager used to build the server |
+----------------+------------------------------------------------------------+
| Python 3       | Required for weather and price forecast scripts            |
+----------------+------------------------------------------------------------+
| Python packages| ``pandas``, ``numpy``, ``fmiopendata``, ``entsoe-py``      |
+----------------+------------------------------------------------------------+
| Julia          | Needed to run optimisation jobs using Predicer             |
+----------------+------------------------------------------------------------+

Each section below explains how to install these dependencies on both Windows and Unix systems.

Install Rust and Cargo
----------------------
Hertta is a Rust project, so you need the Rust compiler (``rustc``) and package manager (``cargo``).
The recommended installation method is via ``rustup``.

**Windows**

- Download and run the rustup-init.exe installer: https://www.rust-lang.org/tools/install
  or via Winget::

    winget install rustlang.rustup

- Choose *Default installation* to install the stable toolchain.
- After installation, restart your terminal and verify::

    rustc --version
    cargo --version

If the commands are not found, ensure that ``%USERPROFILE%\.cargo\bin`` is in your PATH.

**Linux or macOS**

Install using curl::

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

Follow the on-screen prompts and select the default installation.
When finished, restart your terminal or run ``source $HOME/.cargo/env`` and verify::

    rustc --version
    cargo --version

If you get a *command not found* error, add ``$HOME/.cargo/bin`` to your PATH.

Install Python 3
----------------
Hertta uses Python scripts to fetch weather and price data. First check whether Python is already
installed::

    python --version
    # or
    python3 --version

If the command prints a version (e.g. ``Python 3.11.4``), you can skip ahead.
If Python is missing, follow the instructions for your system.

**Windows**

- Download the latest stable Python 3 installer from the official website: https://www.python.org/downloads/
- Run the installer and check *"Add Python to PATH"* during installation.
- After installation, open a new command prompt and verify the installation::

    python --version

**Linux or macOS**

On Debian/Ubuntu based distributions you can install Python 3 using the package manager::

    sudo apt-get update
    sudo apt-get install python3 python3-dev

Verify with::

    python3 --version

**macOS**

On macOS you can install Python via the official installer or Homebrew::

    brew install python3

Verify with::

    python3 --version

Install Python packages
-----------------------
Hertta's scripts depend on several Python packages. It is recommended to use a virtual
environment to avoid polluting your system Python.

Create and activate a virtual environment::

    python3 -m venv ~/.venvs/hertta
    source ~/.venvs/hertta/bin/activate  # On Windows: .\.venvs\hertta\Scripts\activate

Install the required packages::

    pip install pandas numpy fmiopendata entsoe-py

Install Julia
----------------------------
The recommended way to install Julia is to install juliaup which is a small, self-contained binary that will automatically install the latest stable julia binary and help keep it up to date. It also supports installing and using different versions of Julia simultaneously.

**Windows (via Microsoft Store with juliaup)**

Install juliaup from the Microsoft Store by running this in the command prompt::

    winget install --name Julia --id 9NJNWW8PVKMN -e -s msstore

If you cannot access the Microsoft Store, try the experimental `MSIX App Installer <https://install.julialang.org/Julia.appinstaller>`_



**Linux/macOS (via installer script with juliaup)**::

    curl -fsSL https://install.julialang.org | sh

Verify with::

    julia --version
