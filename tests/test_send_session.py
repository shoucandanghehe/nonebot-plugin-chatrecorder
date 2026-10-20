import sys
from importlib import import_module

import pytest
from nonebot import get_driver
from nonebug import App
from sqlalchemy import select


@pytest.mark.parametrize(
    "adapter_name",
    [
        "onebot_v11",
        pytest.param(
            "milky",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 10), reason="Milky requires Python 3.10+"
            ),
        ),
    ],
)
@pytest.mark.parametrize("private", [False, True])
@pytest.mark.parametrize("existing_bot_session", [False, True])
async def test_send_preserves_received_session(
    app: App,
    recorded_messages,
    adapter_name: str,
    private: bool,
    existing_bot_session: bool,
):
    from nonebot_plugin_orm import get_session
    from nonebot_plugin_uninfo import Member, Scene, SceneType, Session, User
    from nonebot_plugin_uninfo.orm import (
        SceneModel,
        SessionModel,
        UserModel,
        get_session_persist_id,
        get_user_persist_id,
    )

    from nonebot_plugin_chatrecorder.model import MessageRecord

    recorder = import_module(f"nonebot_plugin_chatrecorder.adapters.{adapter_name}")
    if adapter_name == "onebot_v11":
        from nonebot.adapters.onebot.v11 import Adapter, Bot, Message

        from .test_onebot_v11 import (
            fake_group_message_event,
            fake_private_message_event,
        )

        async with app.test_api() as ctx:
            bot = ctx.create_bot(
                base=Bot,
                adapter=get_driver()._adapters[Adapter.get_name()],
                self_id="11",
            )
        event = fake_private_message_event() if private else fake_group_message_event()
        api = "send_private_msg" if private else "send_group_msg"
        result = {"message_id": 2}
    else:
        from nonebot.adapters.milky import Message
        from nonebot.adapters.milky.model.api import MessageResponse

        from .test_milky import _bot, _event

        bot = _bot()
        event = _event(
            "friend" if private else "group",
            10 if private else 10000,
            10,
            1,
            [{"type": "text", "data": {"text": "received"}}],
            group_id=10000,
        )
        api = "send_private_message" if private else "send_group_message"
        result = MessageResponse(message_seq=2, time=1700000001)

    scene = Scene(
        id="10" if private else "10000",
        type=SceneType.PRIVATE if private else SceneType.GROUP,
        name="接收方" if private else "验收群",
    )
    session = Session(
        self_id="11",
        adapter=recorder.adapter,
        scope="QQClient",
        scene=scene,
        user=User(id="10", name="接收方"),
    )
    await recorder.record_recv_msg(bot, event, session)
    received_record = recorded_messages[-1][3]
    bot_user = User(id="11", name="机器人")
    bot_user_id = await get_user_persist_id(session.basic, bot_user)
    if existing_bot_session:
        await get_session_persist_id(
            Session(
                **session.basic,
                scene=scene,
                user=bot_user,
                member=Member(user=bot_user, nick="机器人群名片"),
            )
        )

    async with get_session() as db:
        received_session = await db.get(
            SessionModel, received_record.session_persist_id
        )
        scene_id = received_session.scene_persist_id
        scene_data = (await db.get(SceneModel, scene_id)).scene_data
        user_data = {
            row.id: row.user_data for row in (await db.scalars(select(UserModel))).all()
        }
        member_data = {
            row.id: row.member_data
            for row in (await db.scalars(select(SessionModel))).all()
        }

    # The first reply usually has no (scene, bot-user) session yet.
    for _ in range(2):
        await recorder.record_send_msg(
            bot,
            None,
            api,
            {
                "user_id" if private else "group_id": int(scene.id),
                "message": Message("reply"),
            },
            result,
        )

    async with get_session() as db:
        saved_scene = await db.get(SceneModel, scene_id)
        assert saved_scene.scene_data["name"] == scene.name
        assert saved_scene.scene_data == scene_data
        assert {
            row.id: row.user_data for row in (await db.scalars(select(UserModel))).all()
        } == user_data
        for session_id, original_member_data in member_data.items():
            assert (
                await db.get(SessionModel, session_id)
            ).member_data == original_member_data
        sent_records = (
            await db.scalars(
                select(MessageRecord).where(MessageRecord.type == "message_sent")
            )
        ).all()
        assert len(sent_records) == 2
        for record in sent_records:
            sent_session = await db.get(SessionModel, record.session_persist_id)
            assert sent_session.scene_persist_id == scene_id
            assert sent_session.user_persist_id == bot_user_id


@pytest.mark.parametrize("existing_scene", [False, True])
async def test_send_session_preserves_parent(app: App, existing_scene: bool):
    from nonebot_plugin_orm import get_session
    from nonebot_plugin_uninfo import Scene, SceneType, Session, User
    from nonebot_plugin_uninfo.orm import (
        SceneModel,
        SessionModel,
        get_scene_persist_id,
    )

    from nonebot_plugin_chatrecorder.session import get_send_session_persist_id

    parent = Scene(id="guild", type=SceneType.GUILD, name="服务器")
    scene = Scene(id="channel", type=SceneType.CHANNEL_TEXT, name="频道", parent=parent)
    session = Session(
        self_id="11",
        adapter="OneBot V12",
        scope="Discord",
        scene=scene,
        user=User(id="11"),
    )
    if existing_scene:
        original_scene_id = await get_scene_persist_id(session.basic, scene)
    session.scene = Scene(
        id=scene.id,
        type=scene.type,
        parent=Scene(id=parent.id, type=parent.type),
    )
    session_id = await get_send_session_persist_id(session)
    async with get_session() as db:
        saved_session = await db.get(SessionModel, session_id)
        saved_scene = await db.get(SceneModel, saved_session.scene_persist_id)
        saved_parent = await db.get(SceneModel, saved_scene.parent_scene_persist_id)
        assert saved_scene.scene_id == "channel"
        assert saved_parent.scene_id == "guild"
        assert saved_scene.scene_data["name"] == ("频道" if existing_scene else None)
        assert saved_parent.scene_data["name"] == ("服务器" if existing_scene else None)
        if existing_scene:
            assert saved_scene.id == original_scene_id
        parent_id = saved_parent.id

    # A later send may omit parent information entirely.
    session.scene.parent = None
    assert await get_send_session_persist_id(session) == session_id
    async with get_session() as db:
        assert (
            await db.get(SceneModel, saved_scene.id)
        ).parent_scene_persist_id == parent_id
        assert len((await db.scalars(select(SceneModel))).all()) == 2
