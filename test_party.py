"""Local test of parties + online doubles (and that 1v1 still works).

    python test_party.py            # starts server.py on PORT=8792, runs everything, stops it
    PB_TEST_URL=ws://host:port python test_party.py   # against an already running server
"""
import asyncio, json, os, subprocess, sys, time
from websockets.asyncio.client import connect

PORT = 8792
URL = os.environ.get("PB_TEST_URL", "ws://127.0.0.1:%d" % PORT)


class C:
    def __init__(self, code, ovr=60):
        self.code, self.ovr = code, ovr
        self.box = []          # unread messages: dicts (JSON) or bytes
        self.ev = asyncio.Event()

    async def open(self):
        self.ws = await connect(URL, max_size=4 * 1024 * 1024)
        self.task = asyncio.create_task(self._rx())
        await self.ctrl(type="hello", id=self.code, code=self.code, name=self.code, ovr=self.ovr, friends=[], version=1)
        await self.want("welcome")
        return self

    async def _rx(self):
        try:
            async for m in self.ws:
                self.box.append(m if isinstance(m, bytes) else json.loads(m))
                self.ev.set()
        except Exception:
            pass

    async def ctrl(self, **m):
        await self.ws.send(json.dumps(m))

    async def want(self, pred, timeout=15.0):
        """Wait for (and consume) the first message matching pred (a type string, bytes, or callable)."""
        if isinstance(pred, str):
            t = pred
            pred = lambda m: isinstance(m, dict) and m.get("type") == t
        elif isinstance(pred, bytes):
            b = pred
            pred = lambda m: m == b
        end = time.time() + timeout
        while True:
            for i, m in enumerate(self.box):
                if pred(m):
                    return self.box.pop(i)
            left = end - time.time()
            if left <= 0:
                raise AssertionError("%s: timed out; unread=%r" % (self.code, self.box[-6:]))
            self.ev.clear()
            try:
                await asyncio.wait_for(self.ev.wait(), left)
            except asyncio.TimeoutError:
                pass

    async def none(self, pred, wait=0.6):
        await asyncio.sleep(wait)
        bad = [m for m in self.box if pred(m)]
        assert not bad, "%s: unexpected %r" % (self.code, bad)

    def drain(self):
        self.box.clear()

    async def close(self):
        await self.ws.close()


def party_with(n):
    return lambda m: isinstance(m, dict) and m.get("type") == "party" and len(m.get("members") or []) == n


async def make_party(a, b):
    await a.ctrl(type="party_invite", code=b.code)
    inv = await b.want("party_invite")
    assert inv["code"] == a.code and inv["name"] == a.code and inv["ovr"] == a.ovr, inv
    await b.ctrl(type="party_join", party=inv["party"])
    pa = await a.want(party_with(2))
    pb = await b.want(party_with(2))
    assert pa["leader"] == a.code and pb["party"] == inv["party"], (pa, pb)
    assert [m["code"] for m in pa["members"]] == [a.code, b.code], pa
    return inv["party"]


async def ready(*cs):
    for c in cs:
        await c.ctrl(type="party_ready", ready=True)


def check_matched4(ms, expect_codes):
    rooms = {m["room"] for m in ms}
    assert len(rooms) == 1, rooms
    for i, m in enumerate(ms):
        assert m["slot"] == i and m["role"] == ("host" if i == 0 else "guest"), m
        assert [p["code"] for p in m["players"]] == expect_codes, (m["players"], expect_codes)
        assert [p["team"] for p in m["players"]] == [0, 0, 1, 1]


async def test_two_parties():
    a1, a2, b1, b2 = [await C(c, o).open() for c, o in (("A1", 70), ("A2", 64), ("B1", 66), ("B2", 69))]
    await make_party(a1, a2)
    await make_party(b1, b2)
    # party relay before a match: unchanged bytes to the other member only
    await a2.ws.send(b"\x01profile")
    await a1.want(b"\x01profile")
    await b1.none(lambda m: isinstance(m, bytes))
    # half-ready party doesn't queue
    await a1.ctrl(type="party_ready", ready=True)
    p = await a2.want(lambda m: isinstance(m, dict) and m.get("type") == "party" and any(x["ready"] for x in m["members"]))
    assert not p["queued"]
    await ready(a2)
    await a1.want("queued2")
    await b1.ctrl(type="queue2", ovr=66)      # any member's queue2 = ready; the server waits for the other
    await b2.ctrl(type="party_ready", ready=True)
    ms = [await c.want("matched4") for c in (a1, a2, b1, b2)]
    check_matched4(ms, ["A1", "A2", "B1", "B2"])
    for c in (a1, a2, b1, b2):
        c.drain()
    # 4-room relay: prefixed with the sender's slot, to everyone else
    await b1.ws.send(b"xyz")
    for c in (a1, a2, b2):
        assert (await c.want(lambda m: isinstance(m, bytes))) == b"\x02xyz"
    await b1.none(lambda m: isinstance(m, bytes))
    await a1.ws.send(b"snap")
    for c in (a2, b1, b2):
        assert (await c.want(lambda m: isinstance(m, bytes))) == b"\x00snap"
    # a guest leaves
    await b2.ctrl(type="leave")
    for c in (a1, a2, b1):
        m = await c.want("player_left")
        assert m["slot"] == 3, m
    await b2.want(party_with(2))                # back in their party
    await a1.ws.send(b"s2")                      # no longer delivered to the leaver
    await b1.want(b"\x00s2")
    await b2.none(lambda m: isinstance(m, bytes))
    # the host leaves: room closes for everyone
    await a1.ctrl(type="leave")
    for c in (a2, b1):
        await c.want("room_closed")
    await a2.want(party_with(2))
    # party persists across matches: requeue works and plays them again
    for c in (a1, a2, b1, b2):
        c.drain()
    await ready(a1, a2, b1, b2)
    ms = [await c.want("matched4") for c in (a1, a2, b1, b2)]
    check_matched4(ms, ["A1", "A2", "B1", "B2"])
    # host ends the match: back to party
    await a1.ctrl(type="back_to_party")
    for c in (a2, b1, b2):
        await c.want("back_to_party")
    await a1.want(party_with(2))
    await b2.want(party_with(2))
    await a2.ws.send(b"after")                   # party relay is live again
    await a1.want(b"after")
    # leave + kick
    await b1.ctrl(type="party_kick", code="B2")
    k = await b2.want(lambda m: isinstance(m, dict) and m.get("type") == "party" and m.get("party") is None)
    assert k.get("kicked") is True, k
    await b1.want(party_with(1))
    await a2.ctrl(type="party_leave")
    await a2.want(lambda m: isinstance(m, dict) and m.get("type") == "party" and m.get("party") is None)
    await a1.want(party_with(1))
    for c in (a1, a2, b1, b2):
        await c.close()
    print("ok  two parties: queue, matched4 slots/teams, prefixed relay, player_left, room_closed, back_to_party, kick/leave")


async def test_party_and_solos():
    p1, p2 = await C("P1", 75).open(), await C("P2", 71).open()
    s1, s2 = await C("S1", 72).open(), await C("S2", 74).open()
    await make_party(p1, p2)
    await s1.ctrl(type="queue2", ovr=72)
    await s1.want("queued2")
    await ready(p1, p2)
    await s2.ctrl(type="queue2", ovr=74)
    ms = [await c.want("matched4") for c in (p1, p2, s1, s2)]
    check_matched4(ms, ["P1", "P2", "S1", "S2"])
    # disconnecting guest -> player_left
    await s2.close()
    for c in (p1, p2, s1):
        assert (await c.want("player_left"))["slot"] == 3
    # host disconnect -> room_closed
    await p1.close()
    for c in (p2, s1):
        await c.want("room_closed")
    await p2.want(party_with(1))
    await p2.close(); await s1.close()
    print("ok  party + 2 solos: party on team A, solos team B, disconnects")


async def test_four_solos():
    cs = [await C("Q%d" % i, o).open() for i, o in enumerate((80, 50, 52, 79))]
    for c in cs:
        await c.ctrl(type="queue2", ovr=c.ovr)
    ms = [None] * 4
    for c in cs:
        m = await c.want("matched4")
        ms[m["slot"]] = m
    codes = [p["code"] for p in ms[0]["players"]]
    # balanced: 80+50 vs 52+79 (gap 0.5) beats 80+79 vs 50+52
    assert codes[0] == "Q0" and set(codes[:2]) == {"Q0", "Q1"}, codes
    for c in cs:
        await c.close()
    print("ok  four solos: balanced teams", codes)


async def test_unqueue_and_decline():
    a, b, c = await C("U1").open(), await C("U2").open(), await C("U3").open()
    await a.ctrl(type="party_invite", code="U3")
    inv = await c.want("party_invite")
    await c.ctrl(type="party_decline", party=inv["party"])
    d = await a.want("party_declined")
    assert d["code"] == "U3"
    await c.ctrl(type="party_join", party=inv["party"])   # declined invite can't be used
    await c.want("error")
    await b.ctrl(type="queue2", ovr=60)
    await b.want(lambda m: isinstance(m, dict) and m.get("type") == "queued2" and m["queued"])
    await b.ctrl(type="unqueue2")
    await b.want(lambda m: isinstance(m, dict) and m.get("type") == "queued2" and not m["queued"])
    await a.ctrl(type="party_invite", code="NOPE")
    await a.want("error")
    for x in (a, b, c):
        await x.close()
    print("ok  decline, unqueue2, offline invite error")


async def test_1v1_unchanged():
    a, b = await C("V1", 60).open(), await C("V2", 61).open()
    await make_party(a, b)                      # being in a party doesn't change 1v1 at all
    await a.ctrl(type="queue", ovr=60)
    await b.ctrl(type="queue", ovr=61)
    ma, mb = await a.want("matched"), await b.want("matched")
    assert ma["role"] == "host" and mb["role"] == "guest" and ma["opponent"]["code"] == "V2", (ma, mb)
    await b.ws.send(b"\x1c\x00pad")
    assert (await a.want(lambda m: isinstance(m, bytes))) == b"\x1c\x00pad"   # no prefix
    await a.ctrl(type="leave")
    await b.want("opponent_left")
    # private invite path
    await a.ctrl(type="invite", code="V2")
    inv = await b.want("invite")
    await b.ctrl(type="invite_reply", room=inv["room"], accept=True)
    assert (await a.want("matched"))["private"] is True
    await b.want("matched")
    await a.close(); await b.close()
    print("ok  1v1 quick match + private invite unchanged (no relay prefix)")


async def run():
    await test_1v1_unchanged()
    await test_two_parties()
    await test_party_and_solos()
    await test_four_solos()
    await test_unqueue_and_decline()


def main():
    proc = None
    if "PB_TEST_URL" not in os.environ:
        env = dict(os.environ, PORT=str(PORT))
        proc = subprocess.Popen([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.py")], env=env)
        time.sleep(1.5)
    try:
        asyncio.run(asyncio.wait_for(run(), 120))
        print("ALL PASSED")
    finally:
        if proc is not None:
            proc.terminate()       # only our own server process
            proc.wait(5)


if __name__ == "__main__":
    main()
