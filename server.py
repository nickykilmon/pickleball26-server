"""Pickleball 26 online service: quick match by OVR, friends (presence + invites) and a match relay.

One WebSocket per player. Text frames are JSON control messages; binary frames are game traffic and are
forwarded untouched to the other player in the same match. No database: friend lists live on each player's
PC and are sent on connect, so a restart (or the free tier napping) loses nothing but the current queue.

Client -> server (JSON):
  hello {id, code, name, ovr, friends:[codes], version}
  queue {ovr} / unqueue
  friend_add {code}            -> the other player (if online) gets friend_request
  invite {code}                -> target gets invite {room, from, name, ovr}
  invite_reply {room, accept}
  leave                        -> leave the current match
  ping
Server -> client (JSON):
  welcome {online:[codes]}, presence {code, online}
  matched {room, role: host|guest, opponent: {name, code, ovr}, private: bool}
  invite {...}, invite_declined {code}, friend_request {code, name}
  opponent_left, error {msg}, pong
"""
import asyncio, json, os, secrets, time
import http
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

VERSION = 1
players = {}          # code -> Player
queue = {}            # code -> time joined
rooms = {}            # room -> [Player, Player]
pending_invites = {}  # room -> (inviter, target)


class Player:
    def __init__(self, ws):
        self.ws = ws
        self.code = ""
        self.name = "PLAYER"
        self.ovr = 50
        self.friends = set()
        self.room = None
        self.partner = None

    async def send(self, **msg):
        try:
            await self.ws.send(json.dumps(msg))
        except Exception:
            pass


def watchers_of(code):
    return [p for p in players.values() if code in p.friends]


async def presence(code, online):
    for p in watchers_of(code):
        await p.send(type="presence", code=code, online=online)


async def start_match(a, b, private):
    room = secrets.token_hex(4)
    for p in (a, b):
        queue.pop(p.code, None)
        p.room = room
    a.partner, b.partner = b, a
    rooms[room] = [a, b]
    # the first player runs the match simulation, the other draws it
    await a.send(type="matched", room=room, role="host", private=private, opponent=dict(name=b.name, code=b.code, ovr=b.ovr))
    await b.send(type="matched", room=room, role="guest", private=private, opponent=dict(name=a.name, code=a.code, ovr=a.ovr))


async def leave_room(p, notify=True):
    if p.room is None:
        return
    room = rooms.pop(p.room, None)
    other = p.partner
    p.room, p.partner = None, None
    if other is not None:
        other.room, other.partner = None, None
        if notify:
            await other.send(type="opponent_left")


async def matcher():
    """Pair waiting players by closest OVR; the allowed gap widens the longer someone waits.
    With only a couple of people searching, everyone ends up paired with whoever is closest."""
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        waiting = sorted((c for c in queue if c in players), key=lambda c: queue[c])
        taken = set()
        for c in waiting:
            if c in taken:
                continue
            me = players[c]
            win_me = 4 + 3 * (now - queue[c])
            best, best_d = None, 1e9
            for o in waiting:
                if o == c or o in taken:
                    continue
                d = abs(players[o].ovr - me.ovr)
                if d < best_d:
                    best, best_d = o, d
            if best is None:
                continue
            win = max(win_me, 4 + 3 * (now - queue[best]))
            if best_d <= win:
                taken.update((c, best))
                await start_match(me, players[best], False)


async def handle(ws):
    me = Player(ws)
    try:
        async for msg in ws:
            if isinstance(msg, bytes):
                if me.partner is not None:
                    try:
                        await me.partner.ws.send(msg)
                    except Exception:
                        pass
                continue
            try:
                m = json.loads(msg)
            except Exception:
                continue
            t = m.get("type")
            if os.environ.get("PB_DEBUG"):
                print("msg", me.code or "?", t, flush=True)
            if t == "hello":
                code = str(m.get("code", ""))[:12].upper()
                if not code:
                    await me.send(type="error", msg="no code")
                    continue
                old = players.get(code)
                if old is not None and old is not me:
                    await leave_room(old)
                    try:
                        await old.ws.close()
                    except Exception:
                        pass
                me.code = code
                me.name = str(m.get("name", "PLAYER"))[:20]
                me.ovr = int(m.get("ovr", 50))
                me.friends = set(str(f).upper() for f in m.get("friends", []))
                players[code] = me
                await me.send(type="welcome", version=VERSION, online=[f for f in me.friends if f in players])
                await presence(code, True)
            elif not me.code:
                continue
            elif t == "ping":
                await me.send(type="pong")
            elif t == "queue":
                me.ovr = int(m.get("ovr", me.ovr))
                await leave_room(me)
                queue[me.code] = time.time()
            elif t == "unqueue":
                queue.pop(me.code, None)
            elif t == "friend_add":
                code = str(m.get("code", "")).upper()
                me.friends.add(code)
                other = players.get(code)
                if other is not None:
                    await other.send(type="friend_request", code=me.code, name=me.name)
                    await me.send(type="presence", code=code, online=True)
                else:
                    await me.send(type="presence", code=code, online=False)
            elif t == "friend_remove":
                me.friends.discard(str(m.get("code", "")).upper())
            elif t == "invite":
                other = players.get(str(m.get("code", "")).upper())
                if other is None:
                    await me.send(type="error", msg="Your friend isn't online.")
                    continue
                room = secrets.token_hex(4)
                pending_invites[room] = (me.code, other.code)
                await other.send(type="invite", room=room, code=me.code, name=me.name, ovr=me.ovr)
            elif t == "invite_reply":
                inv = pending_invites.pop(str(m.get("room", "")), None)
                if inv is None:
                    continue
                host, target = players.get(inv[0]), players.get(inv[1])
                if host is None or target is not me:
                    continue
                if m.get("accept"):
                    await leave_room(host)
                    await leave_room(me)
                    await start_match(host, me, True)
                else:
                    await host.send(type="invite_declined", code=me.code)
            elif t == "leave":
                await leave_room(me)
    except ConnectionClosed:
        pass
    finally:
        if me.code and players.get(me.code) is me:
            players.pop(me.code, None)
            queue.pop(me.code, None)
            await leave_room(me)
            await presence(me.code, False)


def health(connection, request):
    # Render (and browsers) poke the URL over plain HTTP: answer so health checks pass
    if "Sec-WebSocket-Key" not in request.headers:
        msg = "Pickleball 26 server OK - %d online, %d searching, %d matches" % (len(players), len(queue), len(rooms))
        return connection.respond(http.HTTPStatus.OK, msg + chr(10))
    return None


async def main():
    port = int(os.environ.get("PORT", "8765"))
    async with serve(handle, "0.0.0.0", port, process_request=health, max_size=4 * 1024 * 1024,
                                ping_interval=20, ping_timeout=40):
        print("Pickleball 26 server on port", port, flush=True)
        await matcher()


if __name__ == "__main__":
    asyncio.run(main())
