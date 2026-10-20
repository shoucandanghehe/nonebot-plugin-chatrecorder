import json
from typing import TypeVar

from nonebot_plugin_orm import get_session
from nonebot_plugin_uninfo import Scene, Session
from nonebot_plugin_uninfo.orm import BotModel, SceneModel, SessionModel, UserModel
from sqlalchemy import Select, exc, select

PersistedModel = TypeVar(
    "PersistedModel", BotModel, SceneModel, UserModel, SessionModel
)


async def _get_or_create_id(
    statement: Select[tuple[PersistedModel]], model: PersistedModel
) -> int:
    async with get_session() as db_session:
        existing = (await db_session.scalars(statement)).one_or_none()
        if existing is not None:
            return existing.id

    try:
        async with get_session() as db_session:
            db_session.add(model)
            await db_session.commit()
            await db_session.refresh(model)
            return model.id
    except exc.IntegrityError:
        # Another recorder may have inserted the same unique key after our read.
        async with get_session() as db_session:
            return (await db_session.scalars(statement)).one().id


async def _get_send_scene_persist_id(bot_persist_id: int, scene: Scene) -> int:
    parent_id = (
        await _get_send_scene_persist_id(bot_persist_id, scene.parent)
        if scene.parent
        else None
    )
    return await _get_or_create_id(
        select(SceneModel).where(
            SceneModel.bot_persist_id == bot_persist_id,
            SceneModel.scene_id == scene.id,
            SceneModel.scene_type == scene.type.value,
        ),
        SceneModel(
            bot_persist_id=bot_persist_id,
            parent_scene_persist_id=parent_id,
            scene_id=scene.id,
            scene_type=scene.type.value,
            scene_data=json.loads(scene.dump_json()),
        ),
    )


async def get_send_session_persist_id(session: Session) -> int:
    """Resolve send-only IDs without replacing metadata learned from received messages."""
    basic = session.basic
    bot_persist_id = await _get_or_create_id(
        select(BotModel).where(
            BotModel.self_id == session.self_id,
            BotModel.adapter == basic["adapter"].value,
        ),
        BotModel(
            self_id=session.self_id,
            adapter=basic["adapter"].value,
            scope=basic["scope"].value,
        ),
    )
    scene_persist_id = await _get_send_scene_persist_id(bot_persist_id, session.scene)
    user_persist_id = await _get_or_create_id(
        select(UserModel).where(
            UserModel.bot_persist_id == bot_persist_id,
            UserModel.user_id == session.user.id,
        ),
        UserModel(
            bot_persist_id=bot_persist_id,
            user_id=session.user.id,
            user_data=json.loads(session.user.dump_json()),
        ),
    )
    return await _get_or_create_id(
        select(SessionModel).where(
            SessionModel.bot_persist_id == bot_persist_id,
            SessionModel.scene_persist_id == scene_persist_id,
            SessionModel.user_persist_id == user_persist_id,
        ),
        SessionModel(
            bot_persist_id=bot_persist_id,
            scene_persist_id=scene_persist_id,
            user_persist_id=user_persist_id,
            member_data=json.loads(session.member.dump_json())
            if session.member
            else None,
        ),
    )
