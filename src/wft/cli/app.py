import typer

from .commands import admin, config, inventory, run, scripts, tasks

app = typer.Typer(name="wft", help="WFT node diagnostics", no_args_is_help=True)
app.add_typer(admin.app, name="admin")
app.add_typer(config.app, name="config")
app.add_typer(inventory.app, name="inventory")
app.add_typer(scripts.app, name="scripts")
app.add_typer(run.app, name="run")
app.add_typer(tasks.app, name="task")


@app.callback()
def root() -> None:
    """WFT node diagnostics."""
