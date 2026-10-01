"""Milky recording through real dispatch, API hooks, and committed ORM records.

Only the HTTPX network transport is replaced. SDK preprocessing, API response
validation, NoneBot hooks, Uninfo dependency injection, and SQLAlchemy remain real.
"""

import json
import subprocess
import sys
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx
import nonebot
import pytest
from nonebug import App

if sys.version_info < (3, 10):
    pytest.skip("Milky SDK requires Python 3.10+", allow_module_level=True)

from nonebot.adapters.milky import Adapter, Bot, Message, MessageSegment
from nonebot.adapters.milky.config import ClientInfo
from nonebot.adapters.milky.event import MessageEvent
from nonebot.adapters.milky.exception import ActionFailed
from nonebot.adapters.milky.message import (
    Forward,
    Image,
    Mention,
    MentionAll,
    OutgoingForwardedMessage,
    Reply,
    Text,
)
from nonebot.adapters.milky.model.api import MessageResponse


MESSAGE_TIME = 1700000000
INCOMING_SEGMENTS: list[dict[str, Any]] = [
    {
        "type": "reply",
        "data": {
            "message_seq": 7,
            "sender_id": 11,
            "sender_name": "recorder",
            "time": 1699999990,
            "segments": [{"type": "text", "data": {"text": "quoted message"}}],
        },
    },
    {"type": "mention", "data": {"user_id": 11}},
    {"type": "text", "data": {"text": "  received"}},
    {
        "type": "forward",
        "data": {
            "forward_id": "incoming-forward",
            "title": "Conversation title",
            "preview": ["Alice: first", "Bob: second"],
            "summary": "Two forwarded messages",
        },
    },
]
OUTGOING_SEGMENTS: list[dict[str, Any]] = [
    {"type": "text", "data": {"text": "group outbound"}},
    {"type": "mention", "data": {"user_id": 101}},
    {"type": "reply", "data": {"message_seq": 7}},
    {
        "type": "forward",
        "data": {
            "messages": [
                {
                    "user_id": 101,
                    "sender_name": "Alice",
                    "segments": [
                        {"type": "text", "data": {"text": "nested text"}},
                        {"type": "mention", "data": {"user_id": 102}},
                    ],
                    "time": 1699999991,
                },
                {
                    "user_id": 102,
                    "sender_name": "Bob",
                    "segments": [{"type": "reply", "data": {"message_seq": 8}}],
                    "time": None,
                },
            ],
            "title": "Outgoing title",
            "preview": ["Alice: nested text"],
            "summary": "Two nodes",
            "prompt": "Forwarded conversation",
        },
    },
]


def _member(user_id: int, group_id: int) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "nickname": f"member-{user_id}",
        "sex": "unknown",
        "group_id": group_id,
        "card": f"card-{user_id}",
        "title": "",
        "level": 1,
        "role": "member",
        "join_time": 1690000000,
        "last_sent_time": MESSAGE_TIME,
    }


def _event(
    scene: str,
    peer_id: int,
    sender_id: int,
    seq: int,
    segments: list[dict[str, Any]],
    *,
    self_id: int = 11,
    group_id: int = 900,
) -> MessageEvent:
    data: dict[str, Any] = {
        "message_scene": scene,
        "peer_id": peer_id,
        "sender_id": sender_id,
        "message_seq": seq,
        "time": MESSAGE_TIME,
        "segments": deepcopy(segments),
    }
    if scene == "friend":
        data["friend"] = {
            "user_id": sender_id,
            "nickname": f"friend-{sender_id}",
            "sex": "unknown",
            "qid": "friend-qid",
            "remark": "Friend remark",
            "category": {"category_id": 1, "category_name": "Friends"},
        }
    else:
        data["group"] = {
            "group_id": group_id,
            "group_name": f"group-{group_id}",
            "member_count": 5,
            "max_member_count": 200,
        }
        if scene == "group":
            data["group_member"] = _member(sender_id, group_id)
    event = Adapter.json_to_event(
        {
            "event_type": "message_receive",
            "self_id": self_id,
            # The envelope receipt time is deliberately not the message time.
            "time": MESSAGE_TIME + 900,
            "data": data,
        }
    )
    assert isinstance(event, MessageEvent)
    return event


def _ok(data: dict[str, Any]) -> dict[str, Any]:
    return {"status": "ok", "retcode": 0, "data": data}


def _mock_http(
    monkeypatch: pytest.MonkeyPatch, responses: dict[str, dict[str, Any]]
) -> list[tuple[str, dict[str, Any]]]:
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.host == "milky.test"
        assert request.headers["Authorization"] == "Bearer test-token"
        action = request.url.path.removeprefix("/api/")
        requests.append((action, json.loads(request.content)))
        assert action in responses, f"Unexpected Milky API: {action}"
        return httpx.Response(200, json=responses[action])

    # Keep the real driver Session.request and AsyncClient request/response path.
    # NoneBug's fake Adapter._call_api would bypass SDK/HTTP response validation.
    original_client = httpx.AsyncClient

    def client(*args, **kwargs):
        return original_client(*args, transport=httpx.MockTransport(respond), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    return requests


def _bot(self_id: str = "11") -> Bot:
    adapter = nonebot.get_driver()._adapters[Adapter.get_name()]
    return Bot(
        adapter,
        self_id,
        ClientInfo(host="milky.test", access_token="test-token"),
    )


async def _records_by_session():
    from nonebot_plugin_orm import get_session
    from nonebot_plugin_uninfo.orm import get_session_model
    from sqlalchemy import select

    from nonebot_plugin_chatrecorder.model import MessageRecord

    # A fresh session observes committed rows, not a hook's pending ORM objects.
    async with get_session() as db:
        records = (await db.scalars(select(MessageRecord))).all()
    result = {}
    for record in records:
        session = await (
            await get_session_model(record.session_persist_id)
        ).to_session()
        identity = (
            session.self_id,
            session.adapter,
            session.scope,
            session.scene.type,
            session.scene.id,
            session.user.id,
            session.scene.parent.id if session.scene.parent else None,
        )
        assert identity not in result, f"Unexpected duplicate session: {identity}"
        result[identity] = (record, session)
    return result


async def test_dispatch_records_original_message_and_member(app: App, monkeypatch):
    requests = _mock_http(
        monkeypatch,
        {
            "get_message": _ok(
                {
                    "message": {
                        "message_scene": "group",
                        "peer_id": 900,
                        "message_seq": 7,
                        "sender_id": 11,
                        "time": 1699999990,
                        "segments": [
                            {"type": "text", "data": {"text": "quoted message"}}
                        ],
                    }
                }
            )
        },
    )
    event = _event("group", 900, 101, 42, INCOMING_SEGMENTS)
    await _bot().handle_event(event)

    # Prove SDK preprocessing happened before the recorder's postprocessor.
    assert event.to_me
    assert event.message.extract_plain_text() == "received"
    assert not event.message["reply"]
    assert not event.message["mention"]
    assert requests == [
        ("get_message", {"message_scene": "group", "peer_id": 900, "message_seq": 7})
    ]
    records = await _records_by_session()
    assert set(records) == {("11", "Milky", "QQClient", 1, "900", "101", None)}
    record, session = next(iter(records.values()))
    assert record.message_id == "42@group:900"
    assert record.type == "message"
    assert record.time == datetime(2023, 11, 14, 22, 13, 20)
    assert record.message == INCOMING_SEGMENTS
    assert record.plain_text == "  received"
    assert session.user.name == "member-101"
    assert session.member is not None
    assert session.member.roles[0].id == "MEMBER"
    assert session.member.roles[0].level == 1


async def test_same_seq_keeps_users_scenes_bots_and_temp_sessions_separate(
    app: App, monkeypatch
):
    requests = _mock_http(
        monkeypatch, {"get_group_member_info": _ok({"member": _member(103, 902)})}
    )
    # A sequence number is only unique inside its full Uninfo session.
    cases = [
        ("group", 900, 101, 11, 900, "first member"),
        ("group", 900, 102, 11, 900, "second member"),
        ("group", 901, 101, 11, 901, "other group"),
        ("friend", 101, 101, 11, 900, "private"),
        ("temp", 103, 103, 11, 902, "temporary"),
        ("group", 900, 101, 12, 900, "other bot"),
    ]
    for scene, peer, sender, self_id, group, text in cases:
        event = _event(
            scene,
            peer,
            sender,
            42,
            [{"type": "text", "data": {"text": text}}],
            self_id=self_id,
            group_id=group,
        )
        await _bot(str(self_id)).handle_event(event)

    records = await _records_by_session()
    expected = {
        ("11", "Milky", "QQClient", 1, "900", "101", None): (
            "first member",
            "42@group:900",
        ),
        ("11", "Milky", "QQClient", 1, "900", "102", None): (
            "second member",
            "42@group:900",
        ),
        ("11", "Milky", "QQClient", 1, "901", "101", None): (
            "other group",
            "42@group:901",
        ),
        ("11", "Milky", "QQClient", 0, "101", "101", None): (
            "private",
            "42@friend:101",
        ),
        # Uninfo represents Milky temp messages as their originating group.
        ("11", "Milky", "QQClient", 1, "902", "103", None): (
            "temporary",
            "42@temp:103",
        ),
        ("12", "Milky", "QQClient", 1, "900", "101", None): (
            "other bot",
            "42@group:900",
        ),
    }
    assert set(records) == set(expected)
    assert len({record.session_persist_id for record, _ in records.values()}) == 6
    for identity, (text, message_id) in expected.items():
        record, session = records[identity]
        assert record.message_id == message_id
        assert record.type == "message"
        assert record.time == datetime(2023, 11, 14, 22, 13, 20)
        assert record.message == [{"type": "text", "data": {"text": text}}]
        assert record.plain_text == text
        if identity[3] == 0:
            assert session.user.name == "friend-101"
            assert session.member is None
    assert requests == [
        ("get_group_member_info", {"group_id": 902, "user_id": 103, "no_cache": False})
    ]


@pytest.mark.parametrize("api", ["send_private_message", "send_group_message"])
async def test_real_send_api_records_platform_response_and_forward(
    app: App, monkeypatch, api: str
):
    from nonebot_plugin_chatrecorder.message import deserialize_message

    requests = _mock_http(
        monkeypatch, {api: _ok({"message_seq": 43, "time": MESSAGE_TIME + 2})}
    )
    bot = _bot()
    if api == "send_private_message":
        message = "private outbound"
        target = {"user_id": 101}
        identity = ("11", "Milky", "QQClient", 0, "101", "11", None)
        expected = [{"type": "text", "data": {"text": "private outbound"}}]
        wire_message = expected
        plain_text = "private outbound"
        message_id = "43@friend:101"
    else:
        message = (
            Message("group outbound")
            + MessageSegment.mention(101)
            + MessageSegment.reply(7)
            + MessageSegment.forward(
                [
                    MessageSegment.node(
                        101,
                        "Alice",
                        Message("nested text") + MessageSegment.mention(102),
                        1699999991,
                    ),
                    MessageSegment.node(102, "Bob", Message(MessageSegment.reply(8))),
                ],
                title="Outgoing title",
                preview=["Alice: nested text"],
                summary="Two nodes",
                prompt="Forwarded conversation",
            )
        )
        target = {"group_id": 900}
        identity = ("11", "Milky", "QQClient", 1, "900", "11", None)
        expected = OUTGOING_SEGMENTS
        # SDK 1.3.0's wire Forward.dump omits top-level display metadata.
        # The recorder must nevertheless preserve the complete caller message.
        wire_message = OUTGOING_SEGMENTS[:3] + [
            {
                "type": "forward",
                "data": {"messages": OUTGOING_SEGMENTS[3]["data"]["messages"]},
            }
        ]
        plain_text = "group outbound"
        message_id = "43@group:900"

    result = await bot.call_api(api, **target, message=message)
    assert isinstance(result, MessageResponse)
    assert result.message_seq == 43
    assert result.time == MESSAGE_TIME + 2
    assert requests == [(api, {**target, "message": wire_message})]
    records = await _records_by_session()
    assert set(records) == {identity}
    record, _ = records[identity]
    assert record.type == "message_sent"
    assert record.message_id == message_id
    assert record.time == datetime(2023, 11, 14, 22, 13, 22)
    assert record.message == expected
    assert record.plain_text == plain_text
    # No custom JSON encoder is permitted for the persisted public format.
    assert json.loads(json.dumps(record.message)) == expected

    restored = deserialize_message(bot, json.loads(json.dumps(expected)))
    assert isinstance(restored, Message)
    assert isinstance(restored[0], Text)
    assert restored[0].data == {"text": plain_text}
    if api == "send_group_message":
        assert isinstance(restored[1], Mention)
        assert restored[1].data == {"user_id": 101}
        assert isinstance(restored[2], Reply)
        assert restored[2].data == {"message_seq": 7}
        assert isinstance(restored[3], Forward)
        forward = restored[3].data
        assert forward["title"] == "Outgoing title"
        assert forward["preview"] == ["Alice: nested text"]
        assert forward["summary"] == "Two nodes"
        assert forward["prompt"] == "Forwarded conversation"
        first, second = forward["messages"]
        assert isinstance(first, OutgoingForwardedMessage)
        assert (first.user_id, first.sender_name, first.time) == (
            101,
            "Alice",
            1699999991,
        )
        assert isinstance(first.segments[0], Text)
        assert first.segments[0].data == {"text": "nested text"}
        assert isinstance(first.segments[1], Mention)
        assert first.segments[1].data == {"user_id": 102}
        assert isinstance(second, OutgoingForwardedMessage)
        assert (second.user_id, second.sender_name, second.time) == (102, "Bob", None)
        assert isinstance(second.segments[0], Reply)
        assert second.segments[0].data == {"message_seq": 8}


@pytest.mark.parametrize(
    ("api", "target"),
    [
        ("send_private_message", {"user_id": 101}),
        ("send_group_message", {"group_id": 900}),
    ],
)
async def test_failed_send_api_never_commits_record(app: App, monkeypatch, api, target):
    requests = _mock_http(
        monkeypatch,
        {
            api: {
                "status": "failed",
                "retcode": 1001,
                "message": "permission denied",
                # Even a failed envelope with result-shaped data is not success.
                "data": {"message_seq": 44, "time": MESSAGE_TIME + 3},
            }
        },
    )
    with pytest.raises(ActionFailed):
        await _bot().call_api(api, **target, message="must not be recorded")
    assert requests == [
        (
            api,
            {
                **target,
                "message": [{"type": "text", "data": {"text": "must not be recorded"}}],
            },
        )
    ]
    assert await _records_by_session() == {}


async def test_serializer_matches_literal_json_and_restores_incoming_subclasses(
    app: App,
):
    from nonebot_plugin_chatrecorder.message import (
        deserialize_message,
        serialize_message,
    )

    literal = INCOMING_SEGMENTS + [
        {"type": "mention_all", "data": {}},
        {
            "type": "image",
            "data": {
                "resource_id": "image-resource",
                "temp_url": "https://milky.test/image",
                "width": 320,
                "height": 200,
                "summary": "Image preview",
                "sub_type": "normal",
            },
        },
    ]
    serialized = serialize_message("Milky", Message.from_elements(deepcopy(literal)))
    assert serialized == literal
    assert json.loads(json.dumps(serialized)) == literal
    # Deserialize independently authored persisted JSON, not serializer output.
    restored = deserialize_message("Milky", json.loads(json.dumps(literal)))
    assert isinstance(restored, Message)
    assert isinstance(restored[0], Reply)
    assert restored[0].data["message_seq"] == 7
    assert restored[0].data["sender_id"] == 11
    assert restored[0].data["sender_name"] == "recorder"
    assert restored[0].data["time"] == 1699999990
    quoted = restored[0].data["segments"]
    assert isinstance(quoted[0], Text)
    assert quoted[0].data == {"text": "quoted message"}
    assert isinstance(restored[1], Mention)
    assert restored[1].data == {"user_id": 11}
    assert isinstance(restored[2], Text)
    assert restored[2].data == {"text": "  received"}
    assert isinstance(restored[3], Forward)
    assert restored[3].data == {
        "forward_id": "incoming-forward",
        "title": "Conversation title",
        "preview": ["Alice: first", "Bob: second"],
        "summary": "Two forwarded messages",
    }
    assert isinstance(restored[4], MentionAll)
    assert restored[4].data == {}
    assert isinstance(restored[5], Image)
    assert restored[5].data == literal[5]["data"]


async def _disabled_send_probe():
    """Run in a fresh interpreter: hook registration reads startup config."""
    from sqlalchemy import StaticPool
    from sqlalchemy.ext.asyncio import create_async_engine

    directory = TemporaryDirectory(dir=Path(".pytest_cache"))
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{directory.name}/records.sqlite", poolclass=StaticPool
    )
    nonebot.init(
        driver="~fastapi+~websockets+~httpx",
        sqlalchemy_database_url=engine,
        alembic_startup_check=False,
        chatrecorder_record_send_msg=False,
        uninfo_cache=False,
    )
    nonebot.get_driver().register_adapter(Adapter)
    nonebot.require("nonebot_plugin_chatrecorder")
    from nonebot_plugin_orm import init_orm

    from nonebot_plugin_chatrecorder.config import plugin_config

    assert plugin_config.chatrecorder_record_send_msg is False
    try:
        await init_orm()
        with pytest.MonkeyPatch.context() as monkeypatch:
            requests = _mock_http(
                monkeypatch,
                {
                    "send_private_message": _ok(
                        {"message_seq": 45, "time": MESSAGE_TIME}
                    )
                },
            )
            bot = _bot()
            await bot.handle_event(
                _event(
                    "friend",
                    101,
                    101,
                    46,
                    [{"type": "text", "data": {"text": "receiving remains enabled"}}],
                )
            )
            result = await bot.send_private_message(
                user_id=101, message="disabled send"
            )
            assert isinstance(result, MessageResponse)
            assert (result.message_seq, result.time) == (45, MESSAGE_TIME)
            assert requests == [
                (
                    "send_private_message",
                    {
                        "user_id": 101,
                        "message": [
                            {"type": "text", "data": {"text": "disabled send"}}
                        ],
                    },
                )
            ]
            records = await _records_by_session()
            assert set(records) == {("11", "Milky", "QQClient", 0, "101", "101", None)}
            record, _ = next(iter(records.values()))
            assert record.type == "message"
            assert record.message_id == "46@friend:101"
            assert record.message == [
                {"type": "text", "data": {"text": "receiving remains enabled"}}
            ]
            assert record.plain_text == "receiving remains enabled"
    finally:
        await engine.dispose()
        directory.cleanup()


def test_send_recording_disabled_at_startup_in_fresh_process():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import asyncio; from tests.test_milky import _disabled_send_probe; "
            "asyncio.run(_disabled_send_probe())",
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
