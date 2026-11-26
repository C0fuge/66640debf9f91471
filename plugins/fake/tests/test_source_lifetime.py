"""The built-in's opt-in is tested at actual SDK response serialization."""
import os
import sys

from contact_hub_sdk import file_source_released_on_success
from contact_hub_sdk.testing import FakeCore, schema_check
from conftest import PLUGIN_DIR, SCHEMAS


def test_source_copy_is_closed_before_success_and_download_uses_independent_cache(tmp_path):
    selected = tmp_path / "synthetic-source.bin"
    selected.write_bytes(b"synthetic source bytes")
    bootstrap = """
import sys
from pathlib import Path
from contact_hub_sdk import plugin as sdk_plugin, types as dto
from fake_plugin.plugin import FakePlugin
source_path = Path(sys.argv[1])
held = []
released = False
original_open = Path.open
def tracked_open(path, *args, **kwargs):
    if path == source_path:
        assert not released, 'source reopened after successful response'
    stream = original_open(path, *args, **kwargs)
    if path == source_path:
        held.append(stream)
    return stream
Path.open = tracked_open
original_wire = sdk_plugin.to_wire
def checked_wire(result):
    global released
    if isinstance(result, dto.MessageResult) and result.message.media is not None:
        assert held and all(stream.closed for stream in held), 'response preceded source closure'
        released = True
    return original_wire(result)
sdk_plugin.to_wire = checked_wire
FakePlugin().run()
"""
    with FakeCore([sys.executable, "-c", bootstrap, str(selected)], plugin_dir=PLUGIN_DIR,
                  tmp=tmp_path, env={**os.environ, "HOME":str(tmp_path / "home")},
                  check=schema_check(SCHEMAS)) as core:
        assert file_source_released_on_success(core.initialize()["capabilities"])
        core.start("fake", {"start_state":"connected", "chats":6, "messages_per_chat":0,
                            "transfer_chunk_ms":0, "live_interval_ms":0})
        core.event("status", account_id="fake", state="connected")
        reply = core.call("messages.send_file", {"account_id":"fake", "chat_id":"c5", "path":str(selected), "kind":"document"})
        selected.unlink()
        downloaded = core.call("media.download", {"account_id":"fake", "chat_id":"c5",
                               "message_id":reply["message"]["id"], "what":"full"})
        from pathlib import Path
        assert Path(downloaded["path"]).read_bytes() == b"synthetic source bytes"
        assert core.shutdown() == 0
