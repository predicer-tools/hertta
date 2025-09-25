Running the Hertta Server
=========================

Clone the repository
--------------------
Clone Hertta and init submodules::

    git clone https://github.com/predicer-tools/hertta.git
    cd hertta
    git submodule update --init --recursive

Build the server
----------------
Compile in release mode (first build may take a while)::

    cargo build --release

Generate a settings file
------------------------
Generate default settings and directories::

    cargo run -- --write-settings

This creates ``settings.toml`` under:

- Unix: ``~/.config/hertta/``
- Windows: ``%APPDATA%\hertta``

Adjust the settings file
------------------------
Open ``settings.toml`` and set the following fields appropriately:

- ``python_exec``: path to the Python interpreter (your virtualenv python is recommended).
- ``weather_fetcher_script``: path to ``forecasts/weather_forecast.py``.
- ``price_fetcher_script``: path to ``forecasts/entsoe_forecast.py``.
- ``location``: country and place fields (used for default forecasts).
- ``entsoe_api_token``: your ENTSO-e token for price forecasts.
- Optional: ``julia_exec``, ``predicer_runner_project``, ``predicer_project`` if using Julia/Predicer.

Start the server
----------------
Run the server::

    cargo run

The GraphQL API is available at: http://127.0.0.1:3030/graphql

How to verify the server is running
-----------------------------------
Open the URL above in your browser; the GraphQL playground should be visible.

Configure the Python forecasting scripts
----------------------------------------
Ensure that you installed the required Python packages and that ``python_exec`` in your settings points
to the same environment. You can test the scripts manually::

    # On Unix:
    source ~/.venvs/hertta/bin/activate
    python forecasts/weather_forecast.py --help
    python forecasts/entsoe_forecast.py --help

    # On Windows:
    .\.venvs\hertta\Scripts\activate
    python forecasts\weather_forecast.py --help
    python forecasts\entsoe_forecast.py --help

Running the scripts will create forecast files under the directories configured in ``settings.toml``.
