import json
from datetime import datetime, timezone
from typing import Any, Optional, cast

from nonebot.adapters import Bot as BaseBot
from nonebot.message import event_postprocessor
from nonebot_plugin_orm import get_session
from nonebot_plugin_uninfo import (
    Scene,
    SceneType,
    Session,
    SupportAdapter,
    SupportScope,
    Uninfo,
    User,
)
from nonebot_plugin_uninfo.orm import get_session_persist_id
from typing_extensions import override

from ..config import plugin_config
from ..hooks import save_received_message
from ..message import (
    JsonMsg,
    MessageDeserializer,
    MessageSerializer,
    register_deserializer,
    register_serializer,
    serialize_message,
)
from ..model import MessageRecord
from ..utils import record_type, remove_timezone

try:
    from nonebot.adapters.milky import Bot, Message, MessageEvent
    from nonebot.adapters.milky.message import (
        Forward,
        IncomingForwardData,
        IncomingReplyData,
        OutgoingForwardData,
        OutgoingForwardedMessage,
        Reply,
    )
    from nonebot.adapters.milky.model.api import MessageResponse

    adapter = SupportAdapter.milky

    @event_postprocessor
    async def record_recv_msg(bot: Bot, event: MessageEvent, session: Uninfo):
        session_persist_id = await get_session_persist_id(session)
        message = event.original_message
        record = MessageRecord(
            session_persist_id=session_persist_id,
            time=remove_timezone(datetime.fromtimestamp(event.data.time, timezone.utc)),
            type=record_type(event),
            message_id=f"{event.data.message_seq}@{event.data.message_scene}:{event.data.peer_id}",
            message=serialize_message(adapter, message),
            plain_text=message.extract_plain_text(),
        )
        await save_received_message(bot, event, session, record)

    if plugin_config.chatrecorder_record_send_msg:

        @Bot.on_called_api
        async def record_send_msg(
            bot: BaseBot,
            e: Optional[Exception],
            api: str,
            data: dict[str, Any],
            result: object,
        ):
            if not isinstance(bot, Bot) or e:
                return
            if api == "send_group_message":
                scene = Scene(id=str(data["group_id"]), type=SceneType.GROUP)
                message_scene = "group"
            elif api == "send_private_message":
                scene = Scene(id=str(data["user_id"]), type=SceneType.PRIVATE)
                message_scene = "friend"
            else:
                return

            response = cast(MessageResponse, result)
            session = Session(
                self_id=bot.self_id,
                adapter=adapter,
                scope=SupportScope.qq_client,
                scene=scene,
                user=User(id=bot.self_id),
            )
            session_persist_id = await get_session_persist_id(session)
            message = Message(data["message"])
            record = MessageRecord(
                session_persist_id=session_persist_id,
                time=remove_timezone(
                    datetime.fromtimestamp(response.time, timezone.utc)
                ),
                type="message_sent",
                message_id=f"{response.message_seq}@{message_scene}:{scene.id}",
                message=serialize_message(adapter, message),
                plain_text=message.extract_plain_text(),
            )
            async with get_session() as db_session:
                db_session.add(record)
                await db_session.commit()

    class Serializer(MessageSerializer[Message]):
        @classmethod
        @override
        def serialize(cls, msg: Message) -> JsonMsg:
            # JSON traverses nested Message lists without rebuilding them as dicts.
            # The wire dump would discard forward metadata.
            return json.loads(json.dumps(msg, default=vars))

    class Deserializer(MessageDeserializer[Message]):
        @classmethod
        @override
        def get_message_class(cls) -> type[Message]:
            return Message

        @classmethod
        @override
        def deserialize(cls, msg: JsonMsg) -> Message:
            message = Message()
            for segment in msg:
                data = dict(segment["data"])
                if segment["type"] == "forward":
                    if "messages" in data:
                        data["messages"] = [
                            OutgoingForwardedMessage(
                                user_id=node["user_id"],
                                sender_name=node["sender_name"],
                                segments=list(cls.deserialize(node["segments"])),
                                time=node["time"],
                            )
                            for node in data["messages"]
                        ]
                        message.append(
                            Forward("forward", cast(OutgoingForwardData, data))
                        )
                    else:
                        message.append(
                            Forward("forward", cast(IncomingForwardData, data))
                        )
                elif segment["type"] == "reply" and "segments" in data:
                    data["segments"] = list(cls.deserialize(data["segments"]))
                    message.append(Reply("reply", cast(IncomingReplyData, data)))
                else:
                    message.extend(Message.from_elements([segment]))
            return message

    register_serializer(adapter, Serializer)
    register_deserializer(adapter, Deserializer)

except ImportError:
    pass
