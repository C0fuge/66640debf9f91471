"""Interactive Telegram sign-in through the SDK's per-account flow."""
import asyncio

from contact_hub_sdk import errors as rpc
from telethon import errors as tg


async def phone_login(client, flow):
    phone_error = None
    while True:
        phone = await flow.ask("phone", title="Telegram phone number", error=phone_error)
        try:
            delivery = await client.send_code_request(phone)
        except tg.PhoneNumberInvalidError:
            phone_error = "Enter a phone number with its country code."
            continue
        if not getattr(delivery, "phone_code_hash", None):
            if await client.get_me() is not None:
                return
            raise rpc.AuthRequired("This account requires another sign-in method.")
        code_error = None
        while True:
            code = await flow.ask("code", title="Telegram sign-in code", error=code_error)
            try:
                await client.sign_in(phone=phone, code=code, phone_code_hash=delivery.phone_code_hash)
                return
            except (tg.PhoneCodeInvalidError, tg.PhoneCodeEmptyError):
                code_error = "The code was rejected. Enter it again."
            except tg.PhoneCodeExpiredError:
                phone_error = "The code expired. Request a new one."
                break


async def qr_login(client, flow):
    login = await client.qr_login()
    while True:
        # Subscribe to the update before displaying the token. Cancellation
        # also removes Telethon's temporary event handler.
        wait = asyncio.create_task(login.wait())
        try:
            await asyncio.sleep(0)
            await flow.qr(login.url, expires_at=login.expires)
            await wait
            return
        except (TimeoutError, tg.AuthTokenExpiredError):
            await login.recreate()
        finally:
            wait.cancel()
            await asyncio.gather(wait, return_exceptions=True)


async def sign_in(client, flow, method):
    try:
        if method == "qr":
            await qr_login(client, flow)
        elif method == "phone_code":
            await phone_login(client, flow)
        else:
            raise rpc.InvalidParams("Unknown Telegram sign-in method.", field="method")
    except tg.SessionPasswordNeededError:
        error = None
        while True:
            password = await flow.ask("password", title="Telegram two-step verification password", error=error)
            try:
                await client.sign_in(password=password)
                return
            except tg.PasswordHashInvalidError:
                error = "The password was rejected. Enter it again."
