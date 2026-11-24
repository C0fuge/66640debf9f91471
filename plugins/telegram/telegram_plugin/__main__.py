import os

from .plugin import TelegramPlugin

if __name__ == "__main__":
    os.umask(0o077)
    TelegramPlugin().run()
