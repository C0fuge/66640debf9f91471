"""Entry point: ``python -m {{module_name}}`` (the ``entry`` in plugin.json)."""

from .plugin import PlatformPlugin

PlatformPlugin().run()
