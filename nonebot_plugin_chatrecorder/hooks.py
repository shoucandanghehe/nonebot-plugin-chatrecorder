from collections.abc import Awaitable, Callable
from typing import TypeVar

from nonebot.adapters import Bot, Event
from nonebot_plugin_orm import get_session
from nonebot_plugin_uninfo import Session

from .model import MessageRecord

MessageRecordedHandler = Callable[[Bot, Event, Session, MessageRecord], Awaitable[None]]
_Handler = TypeVar("_Handler", bound=MessageRecordedHandler)
_handlers: list[MessageRecordedHandler] = []


def on_message_recorded(handler: _Handler) -> _Handler:
    """注册接收消息落库后的处理器，按注册顺序调用，异常向上传递。"""
    _handlers.append(handler)
    return handler


async def save_received_message(
    bot: Bot, event: Event, session: Session, record: MessageRecord
) -> None:
    # ORM defaults to expiring committed objects; handlers need readable fields.
    async with get_session(expire_on_commit=False) as db_session:
        db_session.add(record)
        await db_session.commit()

    for handler in _handlers:
        await handler(bot, event, session, record)
